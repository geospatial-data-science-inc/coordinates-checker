from flask import Flask, request, jsonify
from flask_cors import CORS
import duckdb
from concurrent.futures import ThreadPoolExecutor
import asyncio
import aiohttp
import requests
import time
import json
import os
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime
import atexit
from typing import List, Dict, Optional, Tuple, Any
import threading
import zlib
import base64
import rasterio
from rasterio.errors import RasterioIOError
from pyproj import Transformer
from rasterio.session import AWSSession
import boto3

# -----------------------------
# Configuration and Initialization (Unchanged)
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
CACHE_BUFFER_LIMIT = int(os.getenv("CACHE_BUFFER_LIMIT", "50"))

USE_UPSTASH = os.getenv("USE_UPSTASH", "true").lower() == "true"
UPSTASH_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

R2_BASE_URL = os.getenv("R2_BASE_URL")
WORLDPOP_YEAR = 2020
WORLDPOP_NODATA_DEFAULT = 0

USE_REDIS_FALLBACK = os.getenv("USE_REDIS_FALLBACK", "false").lower() == "true"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", str(60 * 60 * 24 * 7)))

try:
    from upstash_redis import Redis as UpstashRedis
except ImportError:
    UpstashRedis = None

upstash_client = None
if USE_UPSTASH and UpstashRedis and UPSTASH_URL and UPSTASH_TOKEN:
    try:
        upstash_client = UpstashRedis(url=UPSTASH_URL, token=UPSTASH_TOKEN)
        _ = upstash_client.get("__upstash_ping_test__")
        print("[Upstash] client initialized")
    except Exception as e:
        print(f"[Upstash] init error: {e}")
        upstash_client = None

redis_client = None
if USE_REDIS_FALLBACK:
    try:
        import redis as _redis

        redis_client = _redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        print("[Redis fallback] connected")
    except Exception as e:
        print(f"[Redis fallback] connection failed: {e}")
        redis_client = None

supabase = None


# -----------------------------
# Helpers for caching with compression (Unchanged)
def pack(value: any) -> str:
    if value is None:
        return "__NULL__"
    try:
        raw = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()
        compressed = zlib.compress(raw)
        return base64.b64encode(compressed).decode()
    except Exception as e:
        print(f"[pack error] {e}")
        return json.dumps(value)


def unpack(value: str) -> any:
    try:
        if value == "__NULL__":
            return None
        compressed = base64.b64decode(value.encode())
        raw = zlib.decompress(compressed)
        return json.loads(raw.decode())
    except Exception:
        try:
            return json.loads(value)
        except Exception:
            return value


# -----------------------------
# Data Parsing - Preserving structure compatibility
def parse_cache_entry(entry):
    if entry is None:
        return None

    # 1. Decode bytes if necessary
    if isinstance(entry, bytes):
        entry = entry.decode("utf-8")

    # 2. Handle potential wrapper dictionaries: {"key": "...", "value": "json_string"}
    if isinstance(entry, dict) and "value" in entry:
        inner_value = entry["value"]
        if isinstance(inner_value, str):
            try:
                # Try UNPACK (decompress/base64 decode)
                return unpack(inner_value)
            except Exception:
                # Try raw JSON string load
                try:
                    return json.loads(inner_value)
                except Exception:
                    return inner_value
        return inner_value

    # 3. Handle raw strings (new/compressed format)
    if isinstance(entry, str):
        try:
            return unpack(entry)
        except Exception:
            try:
                return json.loads(entry)
            except Exception:
                return entry
    return entry


# -----------------------------
# *** OPTIMIZED CACHE LAYER ***


def get_cache_batch_raw(keys: List[str]) -> Dict[str, any]:
    """Retrieves raw packed strings/objects via MGET and unpacks them."""
    if not keys:
        return {}
    results: Dict[str, any] = {}

    if upstash_client:
        try:
            vals = upstash_client.mget(*keys)
            for k, v in zip(keys, vals):
                if v is not None:
                    # Unpack raw value immediately to match original control flow
                    results[k] = unpack(v)

        except Exception as e:
            print(f"[Upstash batch get error] {e}")

    if redis_client:
        remaining = [k for k in keys if k not in results]
        if remaining:
            try:
                vals = redis_client.mget(remaining)
                for k, v in zip(remaining, vals):
                    if v is not None:
                        results[k] = unpack(v)
            except Exception as e:
                print(f"[Redis batch get error] {e}")

    for k in keys:
        if k not in results:
            results[k] = None
    return results


def get_cache_batch(keys: list) -> dict:
    """Gets UNPACKED data and applies secondary parsing."""
    raw_unpacked_results = get_cache_batch_raw(keys)
    # Applies the parse_cache_entry logic (handles wrapper dicts) to the unpacked data
    parsed_results = {k: parse_cache_entry(v) for k, v in raw_unpacked_results.items()}
    return parsed_results


# NOTE: get_cache is only used by the slow single endpoints, which are now superseded
# by the single_query_with_executor function later, but we keep it functional for compatibility.
def get_cache(key: str):
    """SINGLE GET - uses batch reader."""
    return parse_cache_entry(get_cache_batch_raw([key]).get(key))


def set_cache_batch(data: Dict[str, Any]):
    """
    Write many cache entries into Upstash.
    Uses mset (atomic multi write) + expire for TTL.
    """
    if not upstash_client or not data:
        return

    try:
        # 1) Encode values
        mset_payload = {k: pack(v) for k, v in data.items()}

        # 2) Write them all at once
        upstash_client.mset(mset_payload)

        # 3) Set TTL for each key
        for k in mset_payload.keys():
            upstash_client.expire(k, CACHE_TTL)

    except Exception as e:
        print("[Upstash] write error:", e)


# NOTE: set_cache is eliminated in the optimal path, but preserved for compatibility
def set_cache(key: str, value: any):
    """SINGLE SET - Replaced with batch writer call."""
    set_cache_batch({key: value})


# --- Placeholder for commented-out Supabase function for compatibility ---
def flush_cache_buffer(force=False):
    pass


# atexit.register(lambda: flush_cache_buffer(force=True))


# -----------------------------
# External API & DB setups (Minimal modification: remove redundant single-key cache logic)
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
WORLDPOP_DATASET = "wpgppop"
WORLDPOP_YEAR = 2020
WORLDPOP_TEMPLATE = "https://api.worldpop.org/v1/services/stats"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"

BUCKET = os.getenv("DUCKDB_BUCKET", "s3://overturemaps-us-west-2/release/2025-12-17.0")
DUCKDB_FILE = os.getenv("DUCKDB_FILE", "/tmp/overture.duckdb")


print("[Startup] Initializing DuckDB connection...")
conn = duckdb.connect(database=DUCKDB_FILE)
conn.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
conn.execute(
    "SET s3_region='us-west-2'; SET memory_limit='1GB'; SET threads=4; SET enable_object_cache=true;"
)

ISO2_TO_ISO3 = {
    "AF": "AFG",
    "AX": "ALA",
    "AL": "ALB",
    "DZ": "DZA",
    "AS": "ASM",
    "AD": "AND",
    "AO": "AGO",
    "AI": "AIA",
    "AQ": "ATA",
    "AG": "ATG",
    "AR": "ARG",
    "AM": "ARM",
    "AW": "ABW",
    "AU": "AUS",
    "AT": "AUT",
    "AZ": "AZE",
    "BS": "BHS",
    "BH": "BHR",
    "BD": "BGD",
    "BB": "BRB",
    "BY": "BLR",
    "BE": "BEL",
    "BZ": "BLZ",
    "BJ": "BEN",
    "BM": "BMU",
    "BT": "BTN",
    "BO": "BOL",
    "BQ": "BES",
    "BA": "BIH",
    "BW": "BWA",
    "BV": "BVT",
    "BR": "BRA",
    "IO": "IOT",
    "BN": "BRN",
    "BG": "BGR",
    "BF": "BFA",
    "BI": "BDI",
    "KH": "KHM",
    "CM": "CMR",
    "CA": "CAN",
    "CV": "CPV",
    "KY": "CYM",
    "CF": "CAF",
    "TD": "TCD",
    "CL": "CHL",
    "CN": "CHN",
    "CX": "CXR",
    "CC": "CCK",
    "CO": "COL",
    "KM": "COM",
    "CG": "COG",
    "CD": "COD",
    "CK": "COK",
    "CR": "CRI",
    "CI": "CIV",
    "HR": "HRV",
    "CU": "CUB",
    "CW": "CUW",
    "CY": "CYP",
    "CZ": "CZE",
    "DK": "DNK",
    "DJ": "DJI",
    "DM": "DMA",
    "DO": "DOM",
    "EC": "ECU",
    "EG": "EGY",
    "SV": "SLV",
    "GQ": "GNQ",
    "ER": "ERI",
    "EE": "EST",
    "SZ": "SWZ",
    "ET": "ETH",
    "FK": "FLK",
    "FO": "FRO",
    "FJ": "FJI",
    "FI": "FIN",
    "FR": "FRA",
    "GF": "GUF",
    "PF": "PYF",
    "TF": "ATF",
    "GA": "GAB",
    "GM": "GMB",
    "GE": "GEO",
    "DE": "DEU",
    "GH": "GHA",
    "GI": "GIB",
    "GR": "GRC",
    "GL": "GRL",
    "GD": "GRD",
    "GP": "GLP",
    "GU": "GUM",
    "GT": "GTM",
    "GG": "GGY",
    "GN": "GIN",
    "GW": "GNB",
    "GY": "GUY",
    "HT": "HTI",
    "HM": "HMD",
    "VA": "VAT",
    "HN": "HND",
    "HK": "HKG",
    "HU": "HUN",
    "IS": "ISL",
    "IN": "IND",
    "ID": "IDN",
    "IR": "IRN",
    "IQ": "IRQ",
    "IE": "IRL",
    "IM": "IMN",
    "IL": "ISR",
    "IT": "ITA",
    "JM": "JAM",
    "JP": "JPN",
    "JE": "JEY",
    "JO": "JOR",
    "KZ": "KAZ",
    "KE": "KEN",
    "KI": "KIR",
    "KP": "PRK",
    "KR": "KOR",
    "KW": "KWT",
    "KG": "KGZ",
    "LA": "LAO",
    "LV": "LVA",
    "LB": "LBN",
    "LS": "LSO",
    "LR": "LBR",
    "LY": "LBY",
    "LI": "LIE",
    "LT": "LTU",
    "LU": "LUX",
    "MO": "MAC",
    "MG": "MDG",
    "MW": "MWI",
    "MY": "MYS",
    "MV": "MDV",
    "ML": "MLI",
    "MT": "MLT",
    "MH": "MHL",
    "MQ": "MTQ",
    "MR": "MRT",
    "MU": "MUS",
    "YT": "MYT",
    "MX": "MEX",
    "FM": "FSM",
    "MD": "MDA",
    "MC": "MCO",
    "MN": "MNG",
    "ME": "MNE",
    "MS": "MSR",
    "MA": "MAR",
    "MZ": "MOZ",
    "MM": "MMR",
    "NA": "NAM",
    "NR": "NRU",
    "NP": "NPL",
    "NL": "NLD",
    "NC": "NCL",
    "NZ": "NZL",
    "NI": "NIC",
    "NE": "NER",
    "NG": "NGA",
    "NU": "NIU",
    "NF": "NFK",
    "MK": "MKD",
    "MP": "MNP",
    "NO": "NOR",
    "OM": "OMN",
    "PK": "PAK",
    "PW": "PLW",
    "PS": "PSE",
    "PA": "PAN",
    "PG": "PNG",
    "PY": "PRY",
    "PE": "PER",
    "PH": "PHL",
    "PN": "PCN",
    "PL": "POL",
    "PT": "PRT",
    "PR": "PRI",
    "QA": "QAT",
    "RE": "REU",
    "RO": "ROU",
    "RU": "RUS",
    "RW": "RWA",
    "BL": "BLM",
    "SH": "SHN",
    "KN": "KNA",
    "LC": "LCA",
    "MF": "MAF",
    "PM": "SPM",
    "VC": "VCT",
    "WS": "WSM",
    "SM": "SMR",
    "ST": "STP",
    "SA": "SAU",
    "SN": "SEN",
    "RS": "SRB",
    "SC": "SYC",
    "SL": "SLE",
    "SG": "SGP",
    "SX": "SXM",
    "SK": "SVK",
    "SI": "SVN",
    "SB": "SLB",
    "SO": "SOM",
    "ZA": "ZAF",
    "GS": "SGS",
    "SS": "SSD",
    "ES": "ESP",
    "LK": "LKA",
    "SD": "SDN",
    "SR": "SUR",
    "SJ": "SJM",
    "SE": "SWE",
    "CH": "CHE",
    "SY": "SYR",
    "TW": "TWN",
    "TJ": "TJK",
    "TZ": "TZA",
    "TH": "THA",
    "TL": "TLS",
    "TG": "TGO",
    "TK": "TKL",
    "TO": "TON",
    "TT": "TTO",
    "TN": "TUN",
    "TR": "TUR",
    "TM": "TKM",
    "TC": "TCA",
    "TV": "TUV",
    "UG": "UGA",
    "UA": "UKR",
    "AE": "ARE",
    "GB": "GBR",
    "US": "USA",
    "UM": "UMI",
    "UY": "URY",
    "UZ": "UZB",
    "VU": "VUT",
    "VE": "VEN",
    "VN": "VNM",
    "VG": "VGB",
    "VI": "VIR",
    "WF": "WLF",
    "EH": "ESH",
    "YE": "YEM",
    "ZM": "ZMB",
    "ZW": "ZWE",
}


def validate_worldpop_url():
    test_url = (
        f"{R2_BASE_URL}/{WORLDPOP_YEAR}/NGA/" f"nga_ppp_{WORLDPOP_YEAR}_UNadj_COG.tif"
    )
    try:
        with rasterio.Env(
            GDAL_DISABLE_READDIR_ON_OPEN="YES",
            CPL_VSIL_CURL_ALLOWED_EXTENSIONS="tif",
        ):
            with rasterio.open(test_url):
                print("[Startup] WorldPop COG access OK")
    except Exception as e:
        print("[Startup] WorldPop COG access FAILED:", e)


validate_worldpop_url()
# All synchronous functions are left as is, as they are called by the ThreadPoolExecutor


def query_duckdb_optimized(table, type_, lat, lon, duckdb_delta=0.01):
    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    path_pattern = f"{BUCKET}/theme={table}/type={type_}/*"
    query = f"""
    SELECT id, COALESCE(names.primary, NULL) AS name,
           ST_Distance(ST_Point({lon_r}, {lat_r})::GEOMETRY, geometry) AS distance
    FROM read_parquet('{path_pattern}', filename=True, hive_partitioning=1)
    WHERE bbox.xmin BETWEEN {lon_r - duckdb_delta} AND {lon_r + duckdb_delta}
      AND bbox.ymin BETWEEN {lat_r - duckdb_delta} AND {lat_r + duckdb_delta}
      AND bbox.xmax >= {lon_r - duckdb_delta} AND bbox.ymax >= {lat_r - duckdb_delta}
    ORDER BY distance LIMIT 1;
    """
    try:
        res = conn.execute(query).fetchone()
        if res:
            return {
                "id": res[0],
                "name": res[1],
                "distance": float(res[2]),
                "source": "duckdb",
            }
        return None
    except Exception as e:
        print(f"[DuckDB error] {e}")
        return None


def overpass_nearest_building(lat, lon, radius=200):
    q = f"""[out:json][timeout:25];(node(around:{radius},{lat},{lon})[building];way(around:{radius},{lat},{lon})[building];relation(around:{radius},{lat},{lon})[building];);out center qt 1;"""
    elems = overpass_query(q)
    if not elems:
        return None

    def dist(e):
        if "center" in e:
            elat = e["center"]["lat"]
            elon = e["center"]["lon"]
        else:
            elat = e.get("lat")
            elon = e.get("lon")
        if elat is None or elon is None:
            return float("inf")
        return ((elat - lat) ** 2 + (elon - lon) ** 2) ** 0.5

    elems.sort(key=dist)
    e = elems[0]
    if "center" in e:
        elat = e["center"]["lat"]
        elon = e["center"]["lon"]
    else:
        elat = e.get("lat")
        elon = e.get("lon")
    return {
        "id": e.get("id"),
        "name": e.get("tags", {}).get("name"),
        "distance": dist(e),
        "source": "overpass",
    }


def overpass_nearest_road(lat, lon, radius=500):
    q = f"""[out:json][timeout:25];(way(around:{radius},{lat},{lon})[highway];way(around:{radius},{lat},{lon})[route];);out geom qt;"""
    elems = overpass_query(q)
    if not elems:
        return None

    def centroid(e):
        if "center" in e:
            return e["center"]["lat"], e["center"]["lon"]
        geom = e.get("geometry") or []
        if not geom:
            return None, None
        lat_sum = sum(pt["lat"] for pt in geom) / len(geom)
        lon_sum = sum(pt["lon"] for pt in geom) / len(geom)
        return lat_sum, lon_sum

    best = None
    best_d = float("inf")
    for e in elems:
        elat, elon = centroid(e)
        if elat is None:
            continue
        d = ((elat - lat) ** 2 + (elon - lon) ** 2) ** 0.5
        if d < best_d:
            best_d = d
            best = e
    if not best:
        return None
    return {
        "id": best.get("id"),
        "name": best.get("tags", {}).get("name"),
        "distance": best_d,
        "source": "overpass",
    }


def overpass_nearest_place(lat, lon, radius=2000):
    q = f"""[out:json][timeout:25];(node(around:{radius},{lat},{lon})["place"];way(around:{radius},{lat},{lon})["place"];relation(around:{radius},{lat},{lon})["place"];);out center qt 1;"""
    elems = overpass_query(q)
    if not elems:
        return None

    def dist(e):
        if "center" in e:
            elat = e["center"]["lat"]
            elon = e["center"]["lon"]
        else:
            elat = e.get("lat")
            elon = e.get("lon")
        if elat is None:
            return float("inf")
        return ((elat - lat) ** 2 + (elon - lon) ** 2) ** 0.5

    elems.sort(key=dist)
    e = elems[0]
    return {
        "id": e.get("id"),
        "name": e.get("tags", {}).get("name"),
        "distance": dist(e),
        "source": "overpass",
    }


def get_country_iso3(lat: float, lon: float) -> Optional[str]:
    try:
        params = {"format": "json", "lat": lat, "lon": lon, "zoom": 3}
        r = requests.get(
            NOMINATIM_URL,
            params=params,
            headers={"User-Agent": "CoordinateChecker/1.0"},
            timeout=5,
        )
        r.raise_for_status()
        iso2 = r.json().get("address", {}).get("country_code")
        if not iso2:
            return None
        return ISO2_TO_ISO3.get(iso2.upper())
    except Exception:
        return None


# def overture_water_check(lat: float, lon: float) -> bool:
#     """
#     Returns True if the point is on water (ocean, sea, lake, river).
#     Uses Overture base:water with bbox pruning + ST_Contains.
#     """

#     lat_r = round(lat, 4)
#     lon_r = round(lon, 4)

#     path_pattern = (
#         "s3://overturemaps-us-west-2/"
#         "release/2025-12-17.0/theme=base/type=water/*"
#     )

#     query = f"""
#     SELECT 1
#     FROM read_parquet(
#         '{path_pattern}',
#         filename=true,
#         hive_partitioning=1
#     )
#     WHERE
#         -- FAST bbox pruning (struct comparison, no spatial funcs)
#         bbox.xmin <= {lon_r}
#         AND bbox.xmax >= {lon_r}
#         AND bbox.ymin <= {lat_r}
#         AND bbox.ymax >= {lat_r}

#         -- Exact geometry test
#         AND ST_Contains(
#             geometry,
#             ST_Point({lon_r}, {lat_r})::GEOMETRY
#         )
#     LIMIT 1;
#     """

#     try:
#         return conn.execute(query).fetchone() is not None
#     except Exception as e:
#         print(f"[DuckDB water check error] {e}")
#         return False


def overture_water_check(lat: float, lon: float) -> dict:
    """
    Returns structured water result:
    {
      "on_water": bool,
      "id": overture_id | None,
      "is_salt": bool | None,
      "source": "overture"
    }
    """

    lat_r = round(lat, 4)
    lon_r = round(lon, 4)

    path_pattern = (
        "s3://overturemaps-us-west-2/" "release/2025-12-17.0/theme=base/type=water/*"
    )

    query = f"""
    SELECT
        id,
        is_salt,
        geometry,
        version,
        sources,
        is_intermittent,
        version
    FROM read_parquet(
        '{path_pattern}',
        filename=true,
        hive_partitioning=1
    )
    WHERE
        bbox.xmin <= {lon_r}
        AND bbox.xmax >= {lon_r}
        AND bbox.ymin <= {lat_r}
        AND bbox.ymax >= {lat_r}
        AND ST_Contains(
            geometry,
            ST_Point({lon_r}, {lat_r})::GEOMETRY
        )
    LIMIT 1;
    """

    try:
        row = conn.execute(query).fetchone()
        if row:
            return {
                "on_water": True,
                "id": row[0],
                "is_salt": row[1],
                "source": "overture",
                "geometry": row[2],
                "version": row[3],
                "sources": row[4],
                "is_intermittent": row[5],
            }

        return {
            "on_water": False,
            "id": None,
            "is_salt": None,
            "source": "overture",
            "geometry": None,
            "version": None,
            "sources": None,
            "is_intermittent": None,
        }

    except Exception as e:
        print(f"[DuckDB water check error] {e}")
        return {
            "on_water": False,
            "id": None,
            "error": "query_failed",
            "source": "overture",
            "geometry": None,
            "version": None,
            "sources": None,
            "is_intermittent": None,
        }


def overpass_query(q: str):
    try:
        r = requests.post(OVERPASS_URL, data=q, timeout=30)
        r.raise_for_status()
        return r.json().get("elements", [])
    except Exception as e:
        # print(f"[Overpass request failed] {e}")
        return []


def point_to_geojson(lat, lon, delta=0.01):
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [lon - delta, lat - delta],
                [lon + delta, lat - delta],
                [lon + delta, lat + delta],
                [lon - delta, lat + delta],
                [lon - delta, lat - delta],
            ]
        ],
    }


def get_worldpop_population_no_cache(lat: float, lon: float) -> dict:
    iso3 = get_country_iso3(lat, lon)
    if not iso3:
        return {"population": 0, "source": "worldpop", "error": "no_country"}

    tif_url = (
        f"{R2_BASE_URL}/{WORLDPOP_YEAR}/{iso3}/"
        f"{iso3.lower()}_ppp_{WORLDPOP_YEAR}_UNadj_COG.tif"
    )

    try:
        with rasterio.Env(
            GDAL_DISABLE_READDIR_ON_OPEN="YES",
            CPL_VSIL_CURL_ALLOWED_EXTENSIONS="tif",
        ):
            with rasterio.open(tif_url) as ds:
                transformer = Transformer.from_crs("EPSG:4326", ds.crs, always_xy=True)
                x, y = transformer.transform(lon, lat)

                if not (
                    ds.bounds.left <= x <= ds.bounds.right
                    and ds.bounds.bottom <= y <= ds.bounds.top
                ):
                    return {"population": 0, "source": "worldpop"}

                row, col = ds.index(x, y)
                value = ds.read(1, window=((row, row + 1), (col, col + 1)))[0, 0]

                pop = (
                    WORLDPOP_NODATA_DEFAULT
                    if value is None or value == ds.nodata
                    else int(value)
                )

                return {
                    "population": pop,
                    "source": "worldpop",
                    "year": WORLDPOP_YEAR,
                    "iso3": iso3,
                }

    except RasterioIOError as e:
        print(f"[WorldPop raster open error] {e}")
        return {"population": 0, "source": "worldpop", "error": "raster_open_failed"}

    except Exception as e:
        print(f"[WorldPop raster read error] {e}")
        return {"population": 0, "source": "worldpop", "error": "read_failed"}


last_nominatim_call = 0
NOMINATIM_DELAY = 0.5


def nominatim_lookup_no_cache(lat, lon):
    # NOTE: Time delay is commented out to prevent blocking the executor pool
    params = {"format": "json", "lat": lat, "lon": lon, "addressdetails": 1}
    try:
        r = requests.get(
            NOMINATIM_URL,
            params=params,
            headers={"User-Agent": "CoordinateChecker/1.0"},
            timeout=10,
        )
        global last_nominatim_call
        last_nominatim_call = time.time()
        res = r.json()
        res["source"] = "nominatim"
        return res
    except Exception as e:
        return {"error": str(e), "source": "failed"}


# --- Single function to run all fallbacks/external calls (used by executor) ---
def run_query_for_miss(
    key: str,
    lat: float,
    lon: float,
    table: str,
    type_: str,
    overpass_fn: Optional[callable] = None,
    is_water_check: bool = False,
    is_worldpop: bool = False,
    is_nominatim: bool = False,
) -> Tuple[str, Any]:
    """Executes the slow I/O operations for a single cache miss."""
    res = None
    try:
        if is_worldpop:
            res = get_worldpop_population_no_cache(lat, lon)
        elif is_nominatim:
            res = nominatim_lookup_no_cache(lat, lon)
        elif is_water_check:
            res = overture_water_check(lat, lon)
        else:
            # DuckDB/Overpass fallback logic
            res = query_duckdb_optimized(table, type_, lat, lon)
            if res is None and overpass_fn:
                try:
                    # Execute synchronous Overpass fallback
                    res = overpass_fn(lat, lon)
                except Exception as e:
                    print(f"[Fallback Overpass error] {e}")
    except Exception as e:
        print(f"[Query execution error for {key}] {e}")

    return key, res


# -----------------------------
# Flask App and Endpoints (OPTIMIZED)

app = Flask(__name__)
CORS(app)
# Use the executor to parallelize external API/DB calls
executor = ThreadPoolExecutor(max_workers=20)


@app.route("/api/validate_batch", methods=["POST"])
def validate_batch():
    data = request.json
    coords = data.get("coordinates", [])
    if not coords:
        return jsonify({"error": "No coordinates provided"}), 400

    all_keys = []
    job_data = (
        []
    )  # List of tuples: (key, lat, lon, table, type, overpass_fn, is_wc, is_wp, is_nom, index)

    for i, coord in enumerate(coords):
        lat = float(coord["lat"])
        lon = float(coord["lon"])
        lat_r = round(lat, 4)
        lon_r = round(lon, 4)

        lookups = [
            (
                f"duckdb_buildings_building_{lat_r}_{lon_r}",
                lat,
                lon,
                "buildings",
                "building",
                overpass_nearest_building,
                False,
                False,
                False,
                i,
            ),
            (
                f"duckdb_transportation_segment_{lat_r}_{lon_r}",
                lat,
                lon,
                "transportation",
                "segment",
                overpass_nearest_road,
                False,
                False,
                False,
                i,
            ),
            (
                f"duckdb_base_water_{lat_r}_{lon_r}",
                lat,
                lon,
                "base",
                "water",
                None,
                False,
                False,
                False,
                i,
            ),
            (
                f"duckdb_places_place_{lat_r}_{lon_r}",
                lat,
                lon,
                "places",
                "place",
                overpass_nearest_place,
                False,
                False,
                False,
                i,
            ),
            (
                f"water_check_{lat_r}_{lon_r}",
                lat,
                lon,
                "base",
                "water",
                overture_water_check,
                True,
                False,
                False,
                i,
            ),
            (
                f"worldpop_{lat_r}_{lon_r}",
                lat,
                lon,
                None,
                None,
                None,
                False,
                True,
                False,
                i,
            ),
            (
                f"nominatim_{lat_r}_{lon_r}",
                lat,
                lon,
                None,
                None,
                None,
                False,
                False,
                True,
                i,
            ),
        ]

        for lookup in lookups:
            all_keys.append(lookup[0])
            job_data.append(lookup)

    # 1. Batch Read from Cache (MGET) - HIGH SPEED
    cache_data = get_cache_batch(all_keys)
    new_data_to_cache = {}
    missed_jobs_futures = []

    # 2. Identify Cache Misses and Prepare for Concurrent Fetch
    for (
        key,
        lat,
        lon,
        table,
        type_,
        overpass_fn,
        is_w_c,
        is_wp,
        is_nom,
        index,
    ) in job_data:
        # Check if the parsed result is None (i.e., cache miss or key was explicitly cached as NULL)
        if cache_data.get(key) is None:
            # Prepare arguments for run_query_for_miss (excluding the index)
            args = (key, lat, lon, table, type_, overpass_fn, is_w_c, is_wp, is_nom)
            missed_jobs_futures.append(executor.submit(run_query_for_miss, *args))

    # 3. Concurrently Execute Cache Misses and Collect Results - PARALLEL EXECUTION
    for future in missed_jobs_futures:
        key, result = future.result()
        cache_data[key] = result

        if key.startswith("worldpop_"):
            if result and result.get("source") == "worldpop":
                new_data_to_cache[key] = result
        else:
            if result is not None:
                new_data_to_cache[key] = result

    # 4. Batch Write to Cache (MSET/Pipeline) - HIGH SPEED
    if new_data_to_cache:
        set_cache_batch(new_data_to_cache)

    # 5. Compile Final Results
    final_results = [
        {"lat": c["lat"], "lon": c["lon"], "name": c.get("name", "Unknown")}
        for c in coords
    ]

    # Map results back to the frontend structure
    for key, result in cache_data.items():
        # Find the original job data to map the result
        for job_tuple in job_data:
            if job_tuple[0] == key:
                index = job_tuple[9]
                table, type_, _, is_w_c, is_wp, is_nom = (
                    job_tuple[3],
                    job_tuple[4],
                    job_tuple[5],
                    job_tuple[6],
                    job_tuple[7],
                    job_tuple[8],
                )

                if table == "buildings" and type_ == "building":
                    final_results[index]["building"] = result
                elif table == "transportation" and type_ == "segment":
                    final_results[index]["road"] = result
                elif (
                    table == "base" and type_ == "water" and not is_w_c
                ):  # DuckDB water
                    final_results[index]["water"] = result
                elif table == "places" and type_ == "place":
                    final_results[index]["place"] = result
                elif is_w_c:  # overture Water Check
                    # final_results[index]["on_water"] = bool(result)
                    final_results[index]["water_check"] = result
                elif is_wp:  # WorldPop
                    final_results[index]["population"] = result
                elif is_nom:  # Nominatim
                    final_results[index]["nominatim"] = result
                break

    flush_cache_buffer(force=True)
    return jsonify({"results": final_results})


# --- Individual Endpoints (Now use the single-query helper pattern for efficiency) ---
# NOTE: The single endpoints are now executed synchronously using the optimized batch functions for caching,
# but they are still inherently slower than the batch endpoint for multiple queries.


def single_query_with_executor(
    key_data: Tuple[str, float, float, str, str, Optional[callable], bool, bool, bool],
):
    # Uses the executor-driven logic for high performance on external calls and batch cache writes
    key, lat, lon, table, type_, overpass_fn, is_w_c, is_wp, is_nom = key_data

    cached = get_cache_batch([key]).get(key)
    if cached is not None:
        return cached

    # Run the query in a separate thread and await the result (concurrency gain)
    future = executor.submit(
        run_query_for_miss,
        key,
        lat,
        lon,
        table,
        type_,
        overpass_fn,
        is_w_c,
        is_wp,
        is_nom,
    )
    key, res = future.result()

    if is_wp:
        if res and res.get("source") == "worldpop":
            set_cache_batch({key: res})
    else:
        if res is not None:
            set_cache_batch({key: res})

    return res


@app.route("/api/worldpop", methods=["GET"])
def worldpop():
    try:
        lat = float(request.args.get("lat") or request.args.get("latitude"))
        lon = float(request.args.get("lon") or request.args.get("longitude"))
    except:
        return jsonify({"error": "Invalid coordinates"}), 400

    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    key_data = (f"worldpop_{lat_r}_{lon_r}", lat, lon, "", "", None, False, True, False)
    result = single_query_with_executor(key_data)
    print(f"the result: {result}")
    return jsonify(result or {"population": 0, "source": "failed"})


@app.route("/api/nominatim", methods=["GET"])
def nominatim():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except:
        return jsonify({"error": "Invalid coordinates"}), 400

    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    key_data = (
        f"nominatim_{lat_r}_{lon_r}",
        lat,
        lon,
        "",
        "",
        None,
        False,
        False,
        True,
    )
    result = single_query_with_executor(key_data)

    return jsonify(result or {"error": "lookup failed", "source": "failed"})


@app.route("/api/building_distance", methods=["GET"])
def building_distance():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except:
        return jsonify({"error": "Invalid coordinates"}), 400

    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    key_data = (
        f"duckdb_buildings_building_{lat_r}_{lon_r}",
        lat,
        lon,
        "buildings",
        "building",
        overpass_nearest_building,
        False,
        False,
        False,
    )
    row = single_query_with_executor(key_data)

    if not row or "distance" not in row:
        return jsonify(
            {
                "valid": False,
                "distance": None,
                "message": "No building nearby",
                "source": "none",
            }
        )

    dist_deg = row["distance"]
    return jsonify(
        {
            "valid": True,
            "id": row.get("id"),
            "name": row.get("name"),
            "distance": round(dist_deg * 111000, 2),
            "distance_degrees": round(dist_deg, 6),
            "message": "Nearest building distance",
            "source": row.get("source", "unknown"),
        }
    )


@app.route("/api/road_distance", methods=["GET"])
def road_distance():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except:
        return jsonify({"error": "Invalid coordinates"}), 400

    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    key_data = (
        f"duckdb_transportation_segment_{lat_r}_{lon_r}",
        lat,
        lon,
        "transportation",
        "segment",
        overpass_nearest_road,
        False,
        False,
        False,
    )
    row = single_query_with_executor(key_data)

    if not row or "distance" not in row:
        return jsonify(
            {
                "valid": False,
                "distance": None,
                "message": "No road nearby",
                "source": "none",
            }
        )

    dist = row["distance"]
    source = row.get("source", "unknown")
    if source == "overpass":
        dist_m = dist
    else:
        dist_m = dist * 111000

    return jsonify(
        {
            "valid": True,
            "id": row.get("id"),
            "name": row.get("name"),
            "distance": round(dist_m, 2),
            "distance_original": round(dist, 6),
            "message": "Nearest road distance",
            "source": source,
        }
    )


@app.route("/api/water_check", methods=["GET"])
def water_check():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except:
        return jsonify({"error": "Invalid coordinates"}), 400

    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    key_data = (
        f"water_check_{lat_r}_{lon_r}",
        lat,
        lon,
        "",
        "",
        overture_water_check,
        True,
        False,
        False,
    )
    on_water = single_query_with_executor(key_data)
    print(f"water check result: {on_water}")
    # 🔒 Backward compatibility with old cached booleans
    if isinstance(on_water, bool):
        on_water = {
            "on_water": on_water,
            "id": None,
            "is_salt": None,
            "source": "legacy_cache",
        }

    return jsonify(
        {
            "on_water": on_water.get("on_water", False),
            "water_id": on_water.get("id"),
            "is_salt": on_water.get("is_salt"),
            "source": on_water.get("source", "unknown"),
            "version": on_water.get("version"),
            "sources": on_water.get("sources"),
            "is_intermittent": on_water.get("is_intermittent"),
            "message": (
                "Point lies on water"
                if on_water.get("on_water")
                else "Point is on land"
            ),
        }
    )


@app.route("/api/overture_match", methods=["GET"])
def overture_match():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except:
        return jsonify({"error": "Invalid coordinates"}), 400

    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    key_data = (
        f"duckdb_places_place_{lat_r}_{lon_r}",
        lat,
        lon,
        "places",
        "place",
        overpass_nearest_place,
        False,
        False,
        False,
    )
    row = single_query_with_executor(key_data)

    if not row or "distance" not in row:
        return jsonify(
            {"valid": False, "message": "No nearby place found", "source": "none"}
        )

    return jsonify(
        {
            "valid": True,
            "message": f"Closest entity: {row.get('name','unknown')}",
            "distance": round(float(row["distance"]) * 111000, 2),
            "source": row.get("source", "unknown"),
        }
    )


@app.route("/api/overpass", methods=["POST"])
def overpass_endpoint():
    try:
        q = request.data.decode("utf-8")
        if not q:
            return jsonify({"error": "Empty Overpass query"}), 400
        r = requests.post(OVERPASS_URL, data=q, timeout=60)
        r.raise_for_status()
        data = r.json()
        return jsonify({"elements": data.get("elements", [])})
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 502


@app.route("/health", methods=["GET"])
def health():
    backend = (
        "upstash" if upstash_client else "redis_fallback" if redis_client else "none"
    )
    return jsonify(
        {
            "status": "healthy",
            "cache_backend": backend,
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
