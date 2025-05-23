import fitz  # PyMuPDF
import os
import re
import io
from PIL import Image

def extract_images_from_pdf(pdf_path, output_folder):
    """
    Extract images from a product catalog PDF and save them with exact product names.
    
    Args:
        pdf_path: Path to the PDF file
        output_folder: Directory to save extracted images
    """
    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Open the PDF
    pdf_document = fitz.open(pdf_path)
    
    print(f"PDF has {len(pdf_document)} pages")
    
    # Track processed images to avoid duplicates
    processed_images = set()
    extracted_count = 0
    
    # Process each page
    for page_num in range(len(pdf_document)):
        page = pdf_document[page_num]
        
        # Get text on the page to extract product names
        text = page.get_text()
        
        # Extract images from the page
        image_list = page.get_images(full=True)
        
        # Dictionary to store image rectangles (for matching with text)
        image_rects = {}
        
        # Extract image rectangles
        for img_index, img in enumerate(image_list):
            xref = img[0]
            
            # Skip if we've already processed this image reference
            if xref in processed_images:
                continue
                
            processed_images.add(xref)
            
            # Get image rectangle
            img_rect = page.get_image_bbox(img)
            if img_rect:
                image_rects[img_index] = img_rect
        
        # Find product entries in the text using the PDF's pattern
        # The catalog has product names followed by packaging info (like 500X20)
        # Using a more precise regex to capture full product names
        product_entries = re.findall(r'([A-Za-z][A-Za-z0-9&\-\s\.]+)(?:\s+\d+(?:X|x)\d+)', text)
        
        # Process found product names
        for full_name in product_entries:
            # Clean up the name, preserving the full product name
            full_name = full_name.strip()
            
            # Skip short names or headers
            if len(full_name) < 4 or "Saurbhi - Product Catalogue" in full_name:
                continue
                
            # Get the text location - search for the exact full name
            text_instances = page.search_for(full_name)
            
            if not text_instances:
                continue
                
            # Get the Y position of the product name
            name_y = text_instances[0].y0
            
            # Find closest image to this text
            closest_img_index = None
            min_distance = float('inf')
            
            for img_index, rect in image_rects.items():
                img_y = rect.y0
                distance = abs(img_y - name_y)
                
                if distance < min_distance:
                    min_distance = distance
                    closest_img_index = img_index
            
            # If we found a close image and the distance is reasonable
            # (adjust threshold as needed based on your PDF layout)
            if closest_img_index is not None and min_distance < 100:  # Increased threshold
                img = image_list[closest_img_index]
                xref = img[0]
                
                try:
                    # Extract image and metadata
                    base_image = pdf_document.extract_image(xref)
                    image_bytes = base_image["image"]
                    image_ext = base_image["ext"]
                    
                    # Create a safe filename while preserving the exact product name
                    safe_name = re.sub(r'[\\/*?:"<>|]', "", full_name)
                    safe_name = safe_name.replace(" ", "_")
                    
                    # Save the image
                    file_path = os.path.join(output_folder, f"{safe_name}.{image_ext}")
                    
                    # Add a counter suffix if file already exists
                    counter = 1
                    while os.path.exists(file_path):
                        file_path = os.path.join(output_folder, f"{safe_name}_{counter}.{image_ext}")
                        counter += 1
                    
                    with open(file_path, "wb") as img_file:
                        img_file.write(image_bytes)
                    
                    # Try to open the image to ensure it's valid
                    try:
                        with Image.open(io.BytesIO(image_bytes)) as img:
                            width, height = img.size
                            # Skip tiny images that might be icons/decorations
                            if width < 20 or height < 20:
                                os.remove(file_path)
                                continue
                    except Exception as e:
                        print(f"Skipping invalid image: {e}")
                        os.remove(file_path)
                        continue
                    
                    print(f"Saved image: {file_path}")
                    extracted_count += 1
                    
                    # Remove this image from available images to avoid duplicates
                    if closest_img_index in image_rects:
                        del image_rects[closest_img_index]
                        
                except Exception as e:
                    print(f"Error extracting image: {e}")
    
    pdf_document.close()
    return extracted_count

if __name__ == "__main__":
    pdf_path = "Product List With Images -2_250504_162409.pdf"  # Update with your file path
    output_folder = "extracted_product_images"
    
    num_images = extract_images_from_pdf(pdf_path, output_folder)
    print(f"Successfully extracted {num_images} product images.")