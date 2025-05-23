import boto3
import base64
import json
import pandas as pd
from io import BytesIO
from PIL import Image
import os

# === Config ===
CSV_PATH = "products.csv"             # Your CSV
PRODUCT_COLUMN = "Product Name"              # Column with product names
OUTPUT_DIR = "generated_images"      # Local output dir
S3_BUCKET = None                     # Optional: e.g. "your-bucket-name"
REGION = "us-west-2"
MODEL_ID = "amazon.titan-image-generator-v2:0"

# === AWS Clients ===
# Create a session with the SSO profile
session = boto3.Session(profile_name="MantraBazaarAdmin")
# Create clients from the session
bedrock = session.client("bedrock-runtime", region_name=REGION)
s3 = session.client("s3") if S3_BUCKET else None

# === Setup ===
os.makedirs(OUTPUT_DIR, exist_ok=True)
df = pd.read_csv(CSV_PATH)
products = df[PRODUCT_COLUMN].dropna().unique()[:10]  # Limit to first 50 products

# === Tracking variables ===
successful_generations = 0
failed_generations = []

# === Generation Loop ===
for i, name in enumerate(products):
    prompt = (
        f"Ultra-premium product photo of {name}, Indian grocery packaging, "
        "studio lighting, white seamless background, photorealistic, high-resolution"
    )

    print(f"[{i+1}/{len(products)}] Generating: {name}")
    body = {
        "taskType": "TEXT_IMAGE",
        "textToImageParams": {
            "text": prompt,
        },
        "imageGenerationConfig": {
            "cfgScale": 7,
            "seed": 42,
            "quality": "premium",  # ensures best possible output
            "width": 1024,    # optimized for product listings
            "height": 1024,   # square aspect ratio for consistent display
            "numberOfImages": 1
        }
    }

    try:
        response = bedrock.invoke_model(
            modelId=MODEL_ID,
            body=json.dumps(body),
            contentType="application/json",
            accept="application/json"
        )
        response_body = json.loads(response.get('body').read().decode('utf-8'))
        
        # Print the structure of the response
        print("\nResponse structure:")
        print("Keys in response_body:", list(response_body.keys()))
        
        images = response_body.get('images', [])
        
        if not images:
            print(f"No images returned in response. Full response: {response_body}")
            raise Exception("No image generated - empty images array")
            
        base64_image = images[0]  # The base64 string is directly in the image array
        if not base64_image:
            print(f"No base64 image data found")
            raise Exception("No base64 image data in response")
            
        img_bytes = base64.b64decode(base64_image)
        img = Image.open(BytesIO(img_bytes))

        filename = f"{name.replace('/', '-')}.png"
        local_path = os.path.join(OUTPUT_DIR, filename)
        img.save(local_path)

        # if S3_BUCKET:
        #     s3.upload_file(local_path, S3_BUCKET, f"products/{filename}")
        successful_generations += 1

    except Exception as e:
        print(f"❌ Error with {name}: {e}")
        failed_generations.append(name)

# === Print Summary ===
print("\n=== Generation Summary ===")
print(f"✅ Successfully generated: {successful_generations} images")
print(f"❌ Failed to generate: {len(failed_generations)} images")
if failed_generations:
    print("\nFailed products:")
    for i, name in enumerate(failed_generations, 1):
        print(f"{i}. {name}")