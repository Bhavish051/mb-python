import pandas as pd
import os
import shutil
from tqdm import tqdm
import glob

# Read the matched_images CSV
df = pd.read_csv('matched_images.csv')

# Create final_images directory if it doesn't exist
os.makedirs('final_images', exist_ok=True)

# Initialize counters
successful_copies = 0
failed_copies = 0

# Iterate through each row in the CSV with progress bar
for index, row in tqdm(df.iterrows(), total=len(df), desc="Copying images"):
    try:
        product_name = row['product_name']  # Assuming 'Product' is the column name for product names
        image_name = row['image_name']      # Assuming 'Image' is the column name for image filenames
        
        # Search for any file that starts with the image_name in the images folder
        possible_files = glob.glob(os.path.join('images', f"{image_name}.*"))
        # Remove thumb versions if regular versions exist
        filtered_files = [f for f in possible_files if not f.endswith('_thumb.jpg') and not f.endswith('_thumb.png')]
        
        if filtered_files:
            # Use the first matching file found
            source_path = filtered_files[0]
            # Get the file extension from the actual file
            _, file_extension = os.path.splitext(source_path)
            
            # Create destination filename using product name and original extension
            dest_filename = f"{product_name}{file_extension}"
            dest_path = os.path.join('final_images', dest_filename)
            
            # Copy and rename the file
            shutil.copy2(source_path, dest_path)
            successful_copies += 1
            print(f"Copied: {os.path.basename(source_path)} -> {dest_filename}")
        else:
            failed_copies += 1
            print(f"Warning: No matching file found for {image_name}")
            
    except Exception as e:
        failed_copies += 1
        print(f"Error processing row {index}: {str(e)}")

print(f"\nProcess completed:")
print(f"Successfully processed: {successful_copies} images")
print(f"Failed to process: {failed_copies} images")
print(f"Total attempted: {successful_copies + failed_copies} images")
