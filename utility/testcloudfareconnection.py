import rasterio
from rasterio.session import AWSSession
import boto3

url = "https://pub-495b7e1908ec40bb9f6476f715a3286c.r2.dev/2020/NGA/nga_ppp_2020_UNadj_COG.tif"

# For public r2.dev URLs, you don’t need credentials
with rasterio.Env():
    with rasterio.open(url) as ds:
        print(ds.count, ds.width, ds.height)
        val = ds.read(1)[0, 0]  # read the top-left pixel
        print(val)
