import pandas as pd
import os
import shutil
from tqdm import tqdm

# Read the matched_images CSV
df = pd.read_csv('matched_images.csv')

# Create final_images directory if it doesn't exist
os.makedirs('final_images', exist_ok=True)

# Iterate through each row in the CSV with progress bar
for index, row in tqdm(df.iterrows(), total=len(df), desc="Copying images"):
    try:
        product_name = row['Product']  # Assuming 'Product' is the column name for product names
        image_file = row['Image']      # Assuming 'Image' is the column name for image filenames
        
        # Construct source and destination paths
        source_path = os.path.join('images', image_file)
        
        # Get the file extension from source file
        _, file_extension = os.path.splitext(image_file)
        
        # Create destination filename using product name and original extension
        dest_filename = f"{product_name}{file_extension}"
        dest_path = os.path.join('final_images', dest_filename)
        
        # Copy and rename the file
        if os.path.exists(source_path):
            shutil.copy2(source_path, dest_path)
            print(f"Copied: {image_file} -> {dest_filename}")
        else:
            print(f"Warning: Source file not found: {source_path}")
            
    except Exception as e:
        print(f"Error processing row {index}: {str(e)}")

print("\nProcess completed. Check the 'final_images' folder for results.")
