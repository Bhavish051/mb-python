import os
import pandas as pd
import boto3
from tqdm import tqdm
import shutil
import time
import re
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError

def process_inventory(inventory_path):
    """Process inventory Excel file and prepare for matching"""
    print(f"Loading inventory from {inventory_path}")
    df = pd.read_excel(inventory_path)
    
    # Ensure we have required columns
    essential_columns = ['product_id', 'name']
    for col in essential_columns:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in inventory")
    
    # Clean up text descriptions and create search terms
    df['search_terms'] = df.apply(create_search_terms, axis=1)
    
    print(f"Processed {len(df)} inventory items")
    return df

def create_search_terms(row):
    """Create searchable terms from product information"""
    terms = []
    
    # Add product name (most important)
    if isinstance(row.get('name'), str):
        terms.append(row['name'])
    
    # Add other descriptive fields if they exist
    for field in ['category', 'description', 'color', 'material', 'size']:
        if field in row and isinstance(row.get(field), str) and not pd.isna(row.get(field)):
            terms.append(str(row[field]))
    
    return ' '.join(terms)

def setup_aws_client():
    """Set up AWS Rekognition client"""
    print("Setting up AWS Rekognition client")
    rekognition = boto3.client('rekognition')
    return rekognition

def detect_labels_for_image(rekognition_client, image_path, max_labels=50, min_confidence=30):
    """Detect labels for a single image using AWS Rekognition"""
    try:
        with open(image_path, 'rb') as image_file:
            image_bytes = image_file.read()
            
        response = rekognition_client.detect_labels(
            Image={'Bytes': image_bytes},
            MaxLabels=max_labels,
            MinConfidence=min_confidence
        )
        
        return response['Labels']
    except ClientError as e:
        print(f"Error processing {image_path}: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error processing {image_path}: {e}")
        return []

def process_image_batch(args):
    """Process a batch of images - for parallel processing"""
    rekognition_client, image_paths, max_labels, min_confidence = args
    results = {}
    
    for image_path in image_paths:
        try:
            labels = detect_labels_for_image(rekognition_client, image_path, max_labels, min_confidence)
            results[image_path] = labels
        except Exception as e:
            print(f"Failed to process {image_path}: {e}")
            results[image_path] = []
    
    return results

def process_images_parallel(rekognition_client, image_folder, max_workers=10, batch_size=20, max_labels=50, min_confidence=30):
    """Process images in parallel using ThreadPoolExecutor"""
    print(f"Processing images from {image_folder} with {max_workers} workers")
    image_paths = []
    
    # Get all image files
    for filename in os.listdir(image_folder):
        if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.webp')):
            image_paths.append(os.path.join(image_folder, filename))
    
    if not image_paths:
        raise ValueError(f"No images found in {image_folder}")
    
    print(f"Found {len(image_paths)} images")
    
    # Split images into batches for workers
    batches = []
    for i in range(0, len(image_paths), batch_size):
        batch = image_paths[i:min(i + batch_size, len(image_paths))]
        batches.append((rekognition_client, batch, max_labels, min_confidence))
    
    # Process batches in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for batch_result in tqdm(executor.map(process_image_batch, batches), total=len(batches)):
            results.update(batch_result)
    
    return results

def extract_filename(path):
    """Extract filename from path"""
    return os.path.basename(path)

def match_images_to_products(image_labels, inventory_df):
    """Match images to products based on detected labels and inventory descriptions"""
    print("Matching images to products...")
    matches = []
    unmatched = []
    
    # Create a function to calculate match score
    def calculate_match_score(labels, search_terms):
        search_terms_lower = search_terms.lower()
        score = 0
        matched_labels = []
        
        for label in labels:
            label_name = label['Name'].lower()
            label_confidence = label['Confidence'] / 100.0  # Convert to 0-1 scale
            
            # Check if label appears in search terms
            if label_name in search_terms_lower:
                # Higher score for exact matches with high confidence
                score += label_confidence * 1.5
                matched_labels.append(label_name)
            
            # Check for partial matches (label words in search terms)
            for word in label_name.split():
                if len(word) > 3 and word in search_terms_lower:
                    score += label_confidence * 0.5
                    if label_name not in matched_labels:
                        matched_labels.append(label_name)
        
        return score, matched_labels
    
    # For each image, find the best matching product
    for image_path, labels in tqdm(image_labels.items()):
        if not labels:
            unmatched.append({
                'image_path': image_path,
                'filename': extract_filename(image_path),
                'reason': 'No labels detected'
            })
            continue
        
        # Find best matching product
        best_score = 0
        best_match = None
        best_matched_labels = []
        
        for idx, row in inventory_df.iterrows():
            search_terms = row['search_terms']
            score, matched_labels = calculate_match_score(labels, search_terms)
            
            if score > best_score:
                best_score = score
                best_match = row
                best_matched_labels = matched_labels
        
        # Threshold for considering a match valid (adjust as needed)
        if best_score >= 1.0 and best_match is not None:
            matches.append({
                'image_path': image_path,
                'filename': extract_filename(image_path),
                'product_id': best_match['product_id'],
                'product_name': best_match['name'],
                'confidence': best_score,
                'matched_labels': ', '.join(best_matched_labels)
            })
        else:
            unmatched.append({
                'image_path': image_path,
                'filename': extract_filename(image_path),
                'best_match_id': best_match['product_id'] if best_match is not None else None,
                'best_match_name': best_match['name'] if best_match is not None else None,
                'confidence': best_score,
                'matched_labels': ', '.join(best_matched_labels),
                'reason': 'Low confidence match'
            })
    
    print(f"Matched {len(matches)} images, {len(unmatched)} unmatched")
    return matches, unmatched

def save_matches(matches, output_folder, rename_pattern="{product_id}_{index}{ext}"):
    """Save matched images with new filenames"""
    os.makedirs(output_folder, exist_ok=True)
    
    # Group by product ID to handle multiple images per product
    product_counts = {}
    
    for match in tqdm(matches, desc="Saving matched images"):
        product_id = str(match['product_id'])
        if product_id not in product_counts:
            product_counts[product_id] = 0
        
        # Get original extension
        _, ext = os.path.splitext(match['filename'])
        
        # Create new filename
        new_filename = rename_pattern.format(
            product_id=product_id,
            product_name=re.sub(r'[^\w]', '_', str(match['product_name'])),
            confidence=int(match['confidence'] * 10),
            index=product_counts[product_id],
            ext=ext
        )
        
        # Copy file
        shutil.copy2(match['image_path'], os.path.join(output_folder, new_filename))
        product_counts[product_id] += 1
    
    print(f"Saved {sum(product_counts.values())} images to {output_folder}")
    return product_counts

def save_results_excel(matches, unmatched, output_path, inventory_df):
    """Save matching results to Excel for review"""
    # Create dataframes
    if matches:
        matches_df = pd.DataFrame(matches)
    else:
        matches_df = pd.DataFrame(columns=['image_path', 'filename', 'product_id', 'product_name', 'confidence', 'matched_labels'])
    
    if unmatched:
        unmatched_df = pd.DataFrame(unmatched)
    else:
        unmatched_df = pd.DataFrame(columns=['image_path', 'filename', 'best_match_id', 'best_match_name', 'confidence', 'matched_labels', 'reason'])
    
    # Summary by product
    if matches:
        # Count images per product
        product_counts = matches_df.groupby('product_id').size().reset_index(name='image_count')
        # Merge with inventory to get product names
        summary_df = pd.merge(product_counts, 
                             inventory_df[['product_id', 'name']], 
                             on='product_id', 
                             how='left')
    else:
        summary_df = pd.DataFrame(columns=['product_id', 'name', 'image_count'])
    
    # Write to Excel
    with pd.ExcelWriter(output_path) as writer:
        matches_df.to_excel(writer, sheet_name='Matched', index=False)
        unmatched_df.to_excel(writer, sheet_name='Unmatched', index=False)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
    
    print(f"Results saved to {output_path}")

def main(image_folder, inventory_path, output_folder, 
         results_file="matching_results.xlsx", 
         rename_pattern="{product_id}_{index}{ext}",
         max_workers=10,
         batch_size=20):
    """Main function to run the entire process"""
    start_time = time.time()
    print("Starting product image matching process with AWS Rekognition")
    
    # Process inventory
    inventory_df = process_inventory(inventory_path)
    
    # Set up AWS client
    rekognition_client = setup_aws_client()
    
    # Process images and get labels
    image_labels = process_images_parallel(
        rekognition_client, 
        image_folder, 
        max_workers=max_workers,
        batch_size=batch_size
    )
    
    # Match images to products
    matches, unmatched = match_images_to_products(image_labels, inventory_df)
    
    # Save matched images with new filenames
    product_counts = save_matches(matches, output_folder, rename_pattern)
    
    # Save results to Excel
    save_results_excel(matches, unmatched, os.path.join(output_folder, results_file), inventory_df)
    
    elapsed_time = time.time() - start_time
    print(f"Process complete in {elapsed_time:.2f} seconds!")
    print(f"Matched {len(matches)} images to {len(product_counts)} unique products")
    print(f"Output saved to {output_folder}")

if __name__ == "__main__":
    # Configuration - CHANGE THESE VALUES
    image_folder = "path/to/your/32k/images"  # Update this
    inventory_path = "path/to/your/inventory.xlsx"  # Update this
    output_folder = "path/to/output/folder"  # Update this
    
    # Performance settings
    max_workers = 10  # Number of parallel threads (increase for faster processing)
    batch_size = 20   # Images per batch (adjust based on your system resources)
    
    # Run the process
    main(
        image_folder=image_folder,
        inventory_path=inventory_path,
        output_folder=output_folder,
        rename_pattern="{product_id}_{index}{ext}",  # Customize filename pattern
        max_workers=max_workers,
        batch_size=batch_size
    )