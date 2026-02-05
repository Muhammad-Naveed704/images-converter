from PIL import Image
from pathlib import Path
import concurrent.futures
from tqdm import tqdm   

input_folder = Path("images")
output_folder = Path("webp_output")
batch_size = 5000       
max_workers = 8          
quality = 80             

output_folder.mkdir(exist_ok=True)

def convert_to_webp(img_path):
    try:
        img = Image.open(img_path)
        webp_path = output_folder / f"{img_path.stem}.webp"
        img.save(webp_path, format="WEBP", quality=quality, optimize=True)
    except Exception as e:
        print(f"Failed: {img_path.name}, Error: {e}")

images = list(input_folder.glob("*.[jp][pn]g"))
total_images = len(images)
print(f"Total images found: {total_images}")

for i in range(0, total_images, batch_size):
    batch_images = images[i:i+batch_size]
    print(f"\nProcessing batch {i//batch_size + 1} ({len(batch_images)} images)...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(tqdm(executor.map(convert_to_webp, batch_images), total=len(batch_images)))

print("\n✅ All images converted to WebP successfully!")

