import boto3
import os

# ========= CONFIG =========
BUCKET = "coordinates-checker"
ENDPOINT = "https://6f7e284237fcf8b10ab3bac07a3aa47d.r2.cloudflarestorage.com"
ACCESS_KEY = "44903711483f428a9c7abf777c104d22"
SECRET_KEY = "86f94e0609a265e6094e94d79536cc14334e22ee042140a3e35434b3720aeafc"

LOCAL_DIR = r"C:\Users\johna\Downloads\Coordinate Checker\data\worldpop\worldpop_2020"

YEAR = "2020"
# ==========================

session = boto3.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

s3 = session.client(
    "s3",
    endpoint_url=ENDPOINT,
)

for fname in os.listdir(LOCAL_DIR):
    if not fname.endswith(".tif"):
        continue

    # Expect filenames like: nga_ppp_2020_UNadj.tif
    iso3 = fname.split("_")[0].upper()

    local_path = os.path.join(LOCAL_DIR, fname)
    remote_key = f"{YEAR}/{iso3}/{fname}"

    print(f"[UPLOAD] {remote_key}")
    s3.upload_file(
        Filename=local_path,
        Bucket=BUCKET,
        Key=remote_key,
        ExtraArgs={"ContentType": "image/tiff"},
    )

print("All uploads complete.")
