# from PIL import Image
# from pathlib import Path
# import concurrent.futures
# from tqdm import tqdm   

# input_folder = Path("images")
# output_folder = Path("webp_output")
# batch_size = 5000       
# max_workers = 8          
# quality = 80             

# output_folder.mkdir(exist_ok=True)

# def convert_to_webp(img_path):
#     try:
#         img = Image.open(img_path)
#         webp_path = output_folder / f"{img_path.stem}.webp"
#         img.save(webp_path, format="WEBP", quality=quality, optimize=True)
#     except Exception as e:
#         print(f"Failed: {img_path.name}, Error: {e}")

# images = list(input_folder.glob("*.[jp][pn]g"))
# total_images = len(images)
# print(f"Total images found: {total_images}")

# for i in range(0, total_images, batch_size):
#     batch_images = images[i:i+batch_size]
#     print(f"\nProcessing batch {i//batch_size + 1} ({len(batch_images)} images)...")
    
#     with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
#         list(tqdm(executor.map(convert_to_webp, batch_images), total=len(batch_images)))

# print("\n✅ All images converted to WebP successfully!")



from PIL import Image, UnidentifiedImageError
from pathlib import Path
import concurrent.futures
from tqdm import tqdm
import shutil
import threading
import hashlib
import warnings

# ================= SETTINGS =================
input_folder = Path("images")
output_folder = Path("webp_output")
duplicate_folder = Path("duplicate_images")
corrupt_folder = Path("corrupt_images")
log_file = Path("corrupt_images.txt")

batch_size = 5000
max_workers = 8
webp_quality = 80
# ============================================

output_folder.mkdir(exist_ok=True)
duplicate_folder.mkdir(exist_ok=True)
corrupt_folder.mkdir(exist_ok=True)

warnings.filterwarnings(
    "ignore",
    message="Palette images with Transparency"
)

log_lock = threading.Lock()
hash_lock = threading.Lock()

seen_hashes = {}

# ============================================

def is_image_file(p: Path) -> bool:
    return p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}


def file_hash(path: Path, chunk_size=8192) -> str:
    md5 = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            md5.update(chunk)
    return md5.hexdigest()


def unique_path(dest_folder: Path, filename: str) -> Path:
    base = Path(filename).stem
    ext = Path(filename).suffix
    candidate = dest_folder / filename
    i = 1
    while candidate.exists():
        candidate = dest_folder / f"{base}_{i}{ext}"
        i += 1
    return candidate


def process_image(img_path: Path):
    try:
        # corruption check
        with Image.open(img_path) as im:
            im.verify()

        img_hash = file_hash(img_path)

        with hash_lock:
            if img_hash in seen_hashes:
                # duplicate → move original image
                dest = unique_path(duplicate_folder, img_path.name)
                shutil.move(str(img_path), str(dest))
                return "duplicate"
            else:
                seen_hashes[img_hash] = img_path

        # reopen after verify
        with Image.open(img_path) as im:
            if im.mode in ("P", "PA"):
                im = im.convert("RGBA")
            else:
                im = im.convert("RGB")

            webp_name = img_path.stem + ".webp"
            webp_path = output_folder / webp_name

            # avoid overwrite
            i = 1
            while webp_path.exists():
                webp_path = output_folder / f"{img_path.stem}_{i}.webp"
                i += 1

            im.save(webp_path, "WEBP", quality=webp_quality)

        return "converted"

    except (UnidentifiedImageError, OSError, ValueError) as e:
        try:
            dest = unique_path(corrupt_folder, img_path.name)
            shutil.move(str(img_path), str(dest))
            move_msg = ""
        except Exception as move_err:
            move_msg = f" | move failed: {move_err}"

        with log_lock:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"{img_path} :: {e}{move_msg}\n")

        return "corrupt"


# ================= LOAD IMAGES =================
images = [p for p in input_folder.rglob("*") if p.is_file() and is_image_file(p)]
total_images = len(images)
print(f"Total images found: {total_images}")

# ================= PROCESS =====================
converted = 0
duplicates = 0
corrupt = 0

for i in range(0, total_images, batch_size):
    batch_images = images[i:i + batch_size]
    print(f"\nProcessing batch {i // batch_size + 1} ({len(batch_images)} images)...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for status in tqdm(executor.map(process_image, batch_images), total=len(batch_images)):
            if status == "converted":
                converted += 1
            elif status == "duplicate":
                duplicates += 1
            elif status == "corrupt":
                corrupt += 1

# ================= SUMMARY =====================
print("\n================ SUMMARY ================")
print(f"Input images : {total_images}")
print(f"WEBP created : {converted}")
print(f"Duplicates   : {duplicates}")
print(f"Corrupt      : {corrupt}")
print("=========================================")

print("\n✅ DONE — All clean images converted to WEBP with same names.")

