from flask import Flask, request, jsonify
from flask_cors import CORS
import duckdb
from functools import lru_cache
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
from typing import List, Dict, Optional, Tuple
import threading
import zlib
import base64

# -----------------------------
# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
CACHE_BUFFER_LIMIT = int(os.getenv("CACHE_BUFFER_LIMIT", "50"))

USE_UPSTASH = os.getenv("USE_UPSTASH", "true").lower() == "true"
UPSTASH_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

USE_REDIS_FALLBACK = os.getenv("USE_REDIS_FALLBACK", "false").lower() == "true"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", str(60*60*24*7)))

# -----------------------------
# Initialize cache clients
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

# supabase: Optional[Client] = None
# if SUPABASE_URL and SUPABASE_KEY:
#     try:
#         supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
#         print("[Supabase] client initialized")
#     except Exception as e:
#         print(f"[Supabase] init error: {e}")
#         supabase = None

# -----------------------------
# Helpers for caching with compression
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
def parse_cache_entry(entry):
    if entry is None:
        return None
    # If entry is already a dict, parse its 'value' field if needed
    if isinstance(entry, dict):
        if 'value' in entry and isinstance(entry['value'], str):
            try:
                # Attempt to unpack compressed/encoded data
                entry['value'] = unpack(entry['value'])
            except Exception:
                pass
        return entry
    # If entry is a string, try to parse as JSON first, then unpack if needed
    if isinstance(entry, str):
        try:
            parsed = json.loads(entry)
            if isinstance(parsed, dict) and 'value' in parsed and isinstance(parsed['value'], str):
                try:
                    parsed['value'] = unpack(parsed['value'])
                except Exception:
                    pass
            return parsed
        except Exception:
            try:
                return unpack(entry)
            except Exception:
                return entry
    return entry




# -----------------------------
# Cache buffer for Supabase
# CACHE_BUFFER = []
# BUFFER_LOCK = threading.Lock()

# def flush_cache_buffer(force=False):
#     global CACHE_BUFFER
#     with BUFFER_LOCK:
#         if not CACHE_BUFFER:
#             return
#         buffer_to_flush = CACHE_BUFFER.copy()
#         CACHE_BUFFER = []
#     try:
#         if supabase:
#             chunk_size = 100
#             for i in range(0, len(buffer_to_flush), chunk_size):
#                 chunk = buffer_to_flush[i:i + chunk_size]
#                 supabase.table("cache").upsert(chunk).execute()
#             # print(f"[Cache] Flushed {len(buffer_to_flush)} items")
#     except Exception as e:
#         print(f"[Cache flush error] {e}")

# atexit.register(lambda: flush_cache_buffer(force=True))

# -----------------------------
def get_cache_batch_raw(keys: List[str]) -> Dict[str, any]:
    if not keys:
        return {}
    results: Dict[str, any] = {}

    # Try Upstash first
    if upstash_client:
        try:
            vals = upstash_client.mget(*keys)
            for k, v in zip(keys, vals):
                if v is not None:
                    results[k] = unpack(v)
        except Exception as e:
            print(f"[Upstash batch get error] {e}")

    # Redis fallback
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

    # # Supabase for rest
    # if supabase:
    #     missing = [k for k in keys if k not in results]
    #     if missing:
    #         try:
    #             res = supabase.table("cache").select("key, value").in_("key", missing).execute()
    #             for item in res.data:
    #                 key = item["key"]; val = item["value"]
    #                 if val is None:
    #                     results[key] = None
    #                 else:
    #                     try:
    #                         parsed = json.loads(val)
    #                     except Exception:
    #                         parsed = val
    #                     results[key] = parsed
    #                     # also write back to redis/Upstash for future
    #                     packed = pack(parsed)
    #                     if upstash_client:
    #                         try: upstash_client.setex(key, CACHE_TTL, packed)
    #                         except: pass
    #                     if redis_client:
    #                         try: redis_client.setex(key, CACHE_TTL, packed)
    #                         except: pass
    #         except Exception as e:
    #             print(f"[Supabase batch get error] {e}")
    #             for k in missing:
    #                 results[k] = None
    
    # Ensure all keys are present
    for k in keys:
        if k not in results:
            results[k] = None
    return results
def get_cache_batch(keys: list) -> dict:
    raw_results = get_cache_batch_raw(keys)  
    parsed_results = {k: parse_cache_entry(v) for k, v in raw_results.items()}
    return parsed_results

def get_cache(key: str):
    return parse_cache_entry(get_cache_batch([key]).get(key))

def set_cache(key: str, value: any):
    packed = pack(value)
    # Upstash
    if upstash_client:
        try:
            upstash_client.setex(key, CACHE_TTL, packed)
        except Exception as e:
            print(f"[Upstash set error] {e}")
    # Redis fallback
    if redis_client:
        try:
            redis_client.setex(key, CACHE_TTL, packed)
        except Exception as e:
            print(f"[Redis set error] {e}")
    # Supabase buffer
    # if supabase:
    #     with BUFFER_LOCK:
    #         cache_value = json.dumps(value) if value is not None else None
    #         # remove duplicates
    #         CACHE_BUFFER[:] = [e for e in CACHE_BUFFER if e.get("key") != key]
    #         CACHE_BUFFER.append({"key": key, "value": cache_value})
    #         if len(CACHE_BUFFER) >= CACHE_BUFFER_LIMIT:
    #             flush_cache_buffer()

# -----------------------------
# External API & DB setups (same as your old code)
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
WORLDPOP_DATASET = "wpgppop"
WORLDPOP_YEAR = 2020
WORLDPOP_TEMPLATE = "https://api.worldpop.org/v1/services/stats"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"

BUCKET = os.getenv("DUCKDB_BUCKET", "s3://overturemaps-us-west-2/release/2025-10-22.0")
DUCKDB_FILE = os.getenv("DUCKDB_FILE", "/tmp/overture.duckdb")

print("[Startup] Initializing DuckDB connection...")
conn = duckdb.connect(database=DUCKDB_FILE)
conn.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
conn.execute("SET s3_region='us-west-2'; SET memory_limit='1GB'; SET threads=4; SET enable_object_cache=true;")

async def overpass_batch_query(queries: List[Tuple[str, dict]]) -> List[dict]:
    async def fetch_one(session, q, m):
        try:
            async with session.post(OVERPASS_URL, data=q, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                d = await resp.json()
                return {'elements': d.get('elements', []), 'metadata': m, 'success': True}
        except Exception as e:
            return {'elements': [], 'metadata': m, 'success': False, 'error': str(e)}
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_one(session, q, m) for q, m in queries]
        return await asyncio.gather(*tasks)

def overpass_batch_sync(queries: List[Tuple[str, dict]]) -> List[dict]:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(overpass_batch_query(queries))
    finally:
        loop.close()

def overpass_query(q: str):
    try:
        r = requests.post(OVERPASS_URL, data=q, timeout=30)
        r.raise_for_status()
        return r.json().get("elements", [])
    except Exception as e:
        print(f"[Overpass request failed] {e}")
        return []

def overpass_nearest_building(lat, lon, radius=200):
    q = f"""
    [out:json][timeout:25];
    (
      node(around:{radius},{lat},{lon})[building];
      way(around:{radius},{lat},{lon})[building];
      relation(around:{radius},{lat},{lon})[building];
    );
    out center qt 1;
    """
    elems = overpass_query(q)
    if not elems:
        return None
    def dist(e):
        if "center" in e:
            elat = e["center"]["lat"]; elon = e["center"]["lon"]
        else:
            elat = e.get("lat"); elon = e.get("lon")
        if elat is None or elon is None: return float("inf")
        return ((elat - lat)**2 + (elon - lon)**2)**0.5
    elems.sort(key=dist)
    e = elems[0]
    if "center" in e:
        elat = e["center"]["lat"]; elon = e["center"]["lon"]
    else:
        elat = e.get("lat"); elon = e.get("lon")
    return {"id": e.get("id"), "name": e.get("tags", {}).get("name"), "distance": dist(e), "source": "overpass"}

def overpass_nearest_road(lat, lon, radius=500):
    q = f"""
    [out:json][timeout:25];
    (
      way(around:{radius},{lat},{lon})[highway];
      way(around:{radius},{lat},{lon})[route];
    );
    out geom qt;
    """
    elems = overpass_query(q)
    if not elems:
        return None
    def centroid(e):
        if "center" in e:
            return e["center"]["lat"], e["center"]["lon"]
        geom = e.get("geometry") or []
        if not geom: return None, None
        lat_sum = sum(pt["lat"] for pt in geom)/len(geom)
        lon_sum = sum(pt["lon"] for pt in geom)/len(geom)
        return lat_sum, lon_sum
    best = None; best_d = float("inf")
    for e in elems:
        elat, elon = centroid(e)
        if elat is None: continue
        d = ((elat - lat)**2 + (elon - lon)**2)**0.5
        if d < best_d:
            best_d = d; best = e
    if not best:
        return None
    return {"id": best.get("id"), "name": best.get("tags", {}).get("name"), "distance": best_d, "source": "overpass"}

def overpass_nearest_place(lat, lon, radius=2000):
    q = f"""
    [out:json][timeout:25];
    (
      node(around:{radius},{lat},{lon})["place"];
      way(around:{radius},{lat},{lon})["place"];
      relation(around:{radius},{lat},{lon})["place"];
    );
    out center qt 1;
    """
    elems = overpass_query(q)
    if not elems:
        return None
    def dist(e):
        if "center" in e:
            elat = e["center"]["lat"]; elon = e["center"]["lon"]
        else:
            elat = e.get("lat"); elon = e.get("lon")
        if elat is None: return float("inf")
        return ((elat - lat)**2 + (elon - lon)**2)**0.5
    elems.sort(key=dist)
    e = elems[0]
    return {"id": e.get("id"), "name": e.get("tags", {}).get("name"), "distance": dist(e), "source": "overpass"}

def overpass_water_check(lat, lon, radius=50):
    q = f"""
    [out:json][timeout:25];
    (
      way(around:{radius},{lat},{lon})["water"];
      relation(around:{radius},{lat},{lon})["water"];
      node(around:{radius},{lat},{lon})[natural=water];
      node(around:{radius},{lat},{lon})[water];
    );
    out qt 1;
    """
    elems = overpass_query(q)
    return len(elems) > 0

def query_duckdb_optimized(table, type_, lat, lon, duckdb_delta=0.01):
    lat_r = round(lat, 4); lon_r = round(lon, 4)
    path_pattern = f"{BUCKET}/theme={table}/type={type_}/*"
    query = f"""
    SELECT id,
           COALESCE(names.primary, NULL) AS name,
           ST_Distance(ST_Point({lon_r}, {lat_r})::GEOMETRY, geometry) AS distance
    FROM read_parquet('{path_pattern}', filename=True, hive_partitioning=1)
    WHERE bbox.xmin BETWEEN {lon_r - duckdb_delta} AND {lon_r + duckdb_delta}
      AND bbox.ymin BETWEEN {lat_r - duckdb_delta} AND {lat_r + duckdb_delta}
      AND bbox.xmax >= {lon_r - duckdb_delta}
      AND bbox.ymax >= {lat_r - duckdb_delta}
    ORDER BY distance
    LIMIT 1;
    """
    try:
        res = conn.execute(query).fetchone()
        if res:
            return {"id": res[0], "name": res[1], "distance": float(res[2]), "source": "duckdb"}
        return None
    except Exception as e:
        print(f"[DuckDB error] {e}")
        return None

def query_with_fallback(table, type_, lat, lon, overpass_fn=None, duckdb_delta=0.01):
    lat_r = round(lat, 4); lon_r = round(lon, 4)
    cache_key = f"duckdb_{table}_{type_}_{lat_r}_{lon_r}"
    cached = get_cache(cache_key)
    print(f"[cache check] {cached}")
    
    # FIX: Extract value from cached result if it's a dict with metadata
    if cached is not None:
        # If cached is a dict with 'value' key (from Supabase), extract it
        if isinstance(cached, dict) and 'value' in cached:
            cached = cached['value']
        # If we got actual data back (not empty string), return it
        if cached and cached != '':
            return cached
    
    # If no valid cache, query DuckDB
    res = query_duckdb_optimized(table, type_, lat, lon, duckdb_delta)
    
    # If DuckDB fails and we have a fallback, try Overpass
    if res is None and overpass_fn:
        try:
            res = overpass_fn(lat, lon)
        except Exception as e:
            print(f"[Fallback Overpass error] {e}")
    
    # Cache the result
    set_cache(cache_key, res)
    return res

def is_point_on_water_with_fallback(lat, lon):
    lat_r = round(lat, 4); lon_r = round(lon, 4)
    cache_key = f"water_check_{lat_r}_{lon_r}"
    cached = get_cache(cache_key)
    
    # FIX: Extract value from cached result if it's a dict with metadata
    if cached is not None:
        if isinstance(cached, dict) and 'value' in cached:
            cached = cached['value']
        # Handle cached boolean values
        if cached is not None and cached != '':
            return bool(cached)
    
    on_water = False
    try:
        on_water = overpass_water_check(lat, lon)
    except Exception as e:
        print(f"[Water check error] {e}")
    set_cache(cache_key, on_water)
    return on_water

def point_to_geojson(lat, lon, delta=0.01):
    return {"type":"Polygon","coordinates":[[
        [lon-delta, lat-delta],
        [lon+delta, lat-delta],
        [lon+delta, lat+delta],
        [lon-delta, lat+delta],
        [lon-delta, lat-delta]
    ]]}

def get_worldpop_population_with_cache(lat, lon):
    lat_r = round(lat, 4); lon_r = round(lon, 4)
    cache_key = f"worldpop_{lat_r}_{lon_r}"
    cached = get_cache(cache_key)
    
    # FIX: Extract value from cached result if it's a dict with metadata
    if cached is not None:
        if isinstance(cached, dict) and 'value' in cached:
            cached = cached['value']
        # If we have actual cached data, return it
        if cached and cached != '':
            return cached
    
    geojson = {"type":"FeatureCollection","features":[{
        "type":"Feature","properties":{},"geometry":point_to_geojson(lat, lon)
    }]}
    params = {"dataset": WORLDPOP_DATASET, "year": WORLDPOP_YEAR,
              "geojson": json.dumps(geojson), "runasync":"false"}
    try:
        r = requests.get(WORLDPOP_TEMPLATE, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        pop = data.get("data", {}).get("total_population", 0)
        result = {"population": pop, "source": "worldpop"}
    except Exception as e:
        print(f"[WorldPop error] {e}")
        result = {"population": 0, "error": "failed", "source": "failed"}
    set_cache(cache_key, result)
    return result

last_nominatim_call = 0
NOMINATIM_DELAY = 0.5

def nominatim_lookup_with_cache(lat, lon):
    global last_nominatim_call
    lat_r = round(lat, 4); lon_r = round(lon, 4)
    cache_key = f"nominatim_{lat_r}_{lon_r}"
    cached = get_cache(cache_key)
    
    # FIX: Extract value from cached result if it's a dict with metadata
    if cached is not None:
        if isinstance(cached, dict) and 'value' in cached:
            cached = cached['value']
        # If we have actual cached data, return it
        if cached and cached != '':
            return cached
    
    elapsed = time.time() - last_nominatim_call
    if elapsed < NOMINATIM_DELAY:
        time.sleep(NOMINATIM_DELAY - elapsed)
    params = {"format":"json","lat":lat,"lon":lon,"addressdetails":1}
    try:
        r = requests.get(NOMINATIM_URL, params=params,
                         headers={"User-Agent":"CoordinateChecker/1.0"}, timeout=10)
        last_nominatim_call = time.time()
        res = r.json()
        res["source"] = "nominatim"
    except Exception as e:
        res = {"error": str(e), "source": "failed"}
    set_cache(cache_key, res)
    return res
# -----------------------------
app = Flask(__name__)
CORS(app)
executor = ThreadPoolExecutor(max_workers=10)


@app.route("/api/validate_batch", methods=["POST"])
def validate_batch():
    data = request.json
    coords = data.get("coordinates", [])
    if not coords:
        return jsonify({"error": "No coordinates provided"}), 400

    all_keys = []
    info_list = []
    for coord in coords:
        lat = float(coord["lat"]); lon = float(coord["lon"])
        lat_r = round(lat, 4); lon_r = round(lon, 4)
        keys = [
            f"duckdb_buildings_building_{lat_r}_{lon_r}",
            f"duckdb_transportation_segment_{lat_r}_{lon_r}",
            f"duckdb_base_water_{lat_r}_{lon_r}",
            f"duckdb_places_place_{lat_r}_{lon_r}",
            f"water_check_{lat_r}_{lon_r}",
            f"worldpop_{lat_r}_{lon_r}",
            f"nominatim_{lat_r}_{lon_r}",
        ]
        all_keys.extend(keys)
        info_list.append((lat, lon, coord.get("name", "Unknown"), keys))

    cache_data = get_cache_batch(all_keys)

    def process_point(lat, lon, name, keys):
        building = cache_data.get(keys[0]) or query_with_fallback("buildings","building",lat,lon,overpass_nearest_building)
        road     = cache_data.get(keys[1]) or query_with_fallback("transportation","segment",lat,lon,overpass_nearest_road)
        water    = cache_data.get(keys[2]) or query_with_fallback("base","water",lat,lon)
        place    = cache_data.get(keys[3]) or query_with_fallback("places","place",lat,lon,overpass_nearest_place)
        on_water = cache_data.get(keys[4]) if cache_data.get(keys[4]) is not None else is_point_on_water_with_fallback(lat,lon)
        population = cache_data.get(keys[5]) or get_worldpop_population_with_cache(lat,lon)
        nom = cache_data.get(keys[6]) or nominatim_lookup_with_cache(lat,lon)

        return {
            "lat": lat,
            "lon": lon,
            "name": name,
            "building": building,
            "road": road,
            "water": water,
            "place": place,
            "on_water": on_water,
            "population": population,
            "nominatim": nom
        }

    results = [process_point(lat, lon, name, keys) for (lat, lon, name, keys) in info_list]
    flush_cache_buffer(force=True)
    return jsonify({"results": results})

@app.route("/api/worldpop", methods=["GET"])
def worldpop():
    try:
        lat = float(request.args.get("lat") or request.args.get("latitude"))
        lon = float(request.args.get("lon") or request.args.get("longitude"))
    except:
        return jsonify({"error": "Invalid coordinates"}), 400
    return jsonify(get_worldpop_population_with_cache(lat, lon))

@app.route("/api/nominatim", methods=["GET"])
def nominatim():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except:
        return jsonify({"error": "Invalid coordinates"}), 400
    return jsonify(nominatim_lookup_with_cache(lat, lon))

@app.route("/api/building_distance", methods=["GET"])
def building_distance():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except:
        return jsonify({"error": "Invalid coordinates"}), 400
    row = query_with_fallback("buildings", "building", lat, lon, overpass_nearest_building)
    #print the row
    print(f"[Building distance] {row}")
    if not row or "distance" not in row:
        return jsonify({"valid": False, "distance": None, "message": "No building nearby", "source": "none"})
    dist_deg = row["distance"]
    return jsonify({
        "valid": True,
        "distance": round(dist_deg * 111000, 2),
        "distance_degrees": round(dist_deg, 6),
        "message": "Nearest building distance",
        "source": row.get("source", "unknown")
    })

@app.route("/api/road_distance", methods=["GET"])
def road_distance():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except:
        return jsonify({"error": "Invalid coordinates"}), 400
    row = query_with_fallback("transportation", "segment", lat, lon, overpass_nearest_road)
    print(f"[road distance] {row}")

    if not row or "distance" not in row:
        return jsonify({"valid": False, "distance": None, "message": "No road nearby", "source": "none"})
    dist = row["distance"]
    source = row.get("source", "unknown")
    if source == "overpass":
        dist_m = dist
    else:
        dist_m = dist * 111000
    return jsonify({
        "valid": True,
        "distance": round(dist_m, 2),
        "distance_original": round(dist, 6),
        "message": "Nearest road distance",
        "source": source
    })

@app.route("/api/water_check", methods=["GET"])
def water_check():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except:
        return jsonify({"error": "Invalid coordinates"}), 400
    on_water = is_point_on_water_with_fallback(lat, lon)
    return jsonify({
        "on_water": on_water,
        "message": "Point lies on water" if on_water else "Point is on land"
    })

@app.route("/api/overture_match", methods=["GET"])
def overture_match():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except:
        return jsonify({"error": "Invalid coordinates"}), 400
    row = query_with_fallback("places","place",lat,lon,overpass_nearest_place)
    if not row or "distance" not in row:
        return jsonify({"valid": False, "message": "No nearby place found", "source": "none"})
    return jsonify({
        "valid": True,
        "message": f"Closest entity: {row.get('name','unknown')}",
        "distance": round(float(row["distance"]), 2),
        "source": row.get("source","unknown")
    })

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
    backend = ("upstash" if upstash_client else
               "redis_fallback" if redis_client else
               "supabase" if supabase else "none")
    return jsonify({
        "status": "healthy",
        "cache_backend": backend,
        "buffer_size": len(CACHE_BUFFER),
        "supabase_connected": bool(supabase)
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
