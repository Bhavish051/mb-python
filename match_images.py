import os
import pandas as pd
from thefuzz import process
from tqdm import tqdm

# Config
PRODUCT_CSV = "products.csv"
IMAGE_FOLDER = "images"
PRODUCT_COL = "Product Name"

# Load data
df = pd.read_csv(PRODUCT_CSV)
product_names = df[PRODUCT_COL].dropna().unique()
image_files = os.listdir(IMAGE_FOLDER)
image_names = [os.path.splitext(f)[0] for f in image_files]

# Match
matches = []
for name in tqdm(product_names, desc="Matching images", unit="product"):
    result = process.extractOne(name, image_names)
    if result is not None:
        match = result[0]
        score = result[1]
        if score > 80:  # Tune threshold
            matches.append((name, match, score))

# Save mapping
matched_df = pd.DataFrame(matches, columns=["product_name", "image_name", "score"])
matched_df.to_csv("matched_images.csv", index=False)
