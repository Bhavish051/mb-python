import pandas as pd
import os
import shutil
from tqdm import tqdm
import glob

# Read the matched_images CSV
df = pd.read_csv('matched_images.csv')

# Create final_images directory if it doesn't exist
os.makedirs('final_images', exist_ok=True)

# Initialize counters and results list
successful_copies = 0
failed_copies = 0
results = []

# Iterate through each row in the CSV with progress bar
for index, row in tqdm(df.iterrows(), total=len(df), desc="Copying images"):
    result_row = {
        'product_name': row['product_name'],
        'image_name': row['image_name'],
        'status': 'Failed',  # Default status
        'copied_file': 'None'  # Default copied file
    }
    
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
            
            # Update result row with success information
            result_row['status'] = 'Success'
            result_row['copied_file'] = dest_filename
            
        else:
            failed_copies += 1
            print(f"Warning: No matching file found for {image_name}")
            result_row['status'] = 'Failed - No matching file found'
            
    except Exception as e:
        failed_copies += 1
        print(f"Error processing row {index}: {str(e)}")
        result_row['status'] = f'Failed - Error: {str(e)}'
    
    results.append(result_row)

# Create a DataFrame from results and save to CSV
results_df = pd.DataFrame(results)
results_df.to_csv('processing_results.csv', index=False)

print(f"\nProcess completed:")
print(f"Successfully processed: {successful_copies} images")
print(f"Failed to process: {failed_copies} images")
print(f"Total attempted: {successful_copies + failed_copies} images")
print(f"\nDetailed results have been saved to 'processing_results.csv'")
