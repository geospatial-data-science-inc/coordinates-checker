import os
import requests

YEAR = 2020
WORKDIR = f"worldpop_{YEAR}"
os.makedirs(WORKDIR, exist_ok=True)

# list of countries you care about (ISO3)
COUNTRIES = ["ZAF", "TZA", "ZMB", "ZWE"]
# https://data.worldpop.org/GIS/Population/Global_2000_2020/2020/MWI/mwi_ppp_2020_UNadj.tif for malawi
def download_country_raster(year, iso, out_dir):
    iso_lower = iso.lower()
    # folder is uppercase ISO, file name is lowercase + prefix
    url = f"https://data.worldpop.org/GIS/Population/Global_2000_2020/{year}/{iso}/{iso_lower}_ppp_{year}_UNadj.tif"
    out_path = os.path.join(out_dir, f"{iso_lower}_ppp_{year}_UNadj.tif")
    
    if os.path.exists(out_path):
        print(f"[SKIP] {out_path} already exists.")
        return

    print(f"[DOWNLOAD] {iso} from {url}")
    resp = requests.get(url, stream=True)
    if resp.status_code == 404:
        print(f"[NOT FOUND] {iso} file does not exist at {url}")
        return

    try:
        resp.raise_for_status()
    except Exception as e:
        print(f"[ERROR] {e}")
        return

    with open(out_path, "wb") as f:
        for chunk in resp.iter_content(1024 * 1024):
            f.write(chunk)
    print(f"[DONE] Downloaded {out_path}")

if __name__ == "__main__":
    for iso in COUNTRIES:
        download_country_raster(YEAR, iso, WORKDIR)