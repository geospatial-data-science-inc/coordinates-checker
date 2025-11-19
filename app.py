from flask import Flask, request, jsonify
from flask_cors import CORS
import duckdb
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
import requests
import time
import json
import os
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime
import atexit

# -----------------------------
# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
CACHE_BUFFER_LIMIT = int(os.getenv("CACHE_BUFFER_LIMIT", "50"))

# -----------------------------
# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = Flask(__name__)
CORS(app)

# -----------------------------
# Thread pool for parallel processing
executor = ThreadPoolExecutor(max_workers=3)

# -----------------------------
# 🔥 BATCH CACHE BUFFER
CACHE_BUFFER = []

def flush_cache_buffer():
    """Write buffered cache entries to Supabase (upsert)."""
    global CACHE_BUFFER
    if not CACHE_BUFFER:
        return
    try:
        supabase.table("cache").upsert(CACHE_BUFFER).execute()
        print(f"[Cache] Flushed {len(CACHE_BUFFER)} items to Supabase")
        CACHE_BUFFER = []
    except Exception as e:
        print(f"[Cache flush error] {e}")

# Ensure flush at process exit
atexit.register(flush_cache_buffer)

# -----------------------------
# Cache helpers using Supabase table
def safe_maybe_single(res):
    """Return res.data if present, otherwise None."""
    try:
        return res.data if hasattr(res, "data") else (res[0] if res else None)
    except Exception:
        return None

def get_cache(key):
    """Load cached value for a given key from Supabase cache table."""
    try:
        res = supabase.table("cache").select("value").eq("key", key).maybe_single().execute()
        data = safe_maybe_single(res)
        if not data:
            return None
        val = data.get("value") if isinstance(data, dict) and "value" in data else data
        if val is None:
            return None
        if isinstance(val, str):
            try:
                return json.loads(val)
            except Exception:
                return val
        return val
    except Exception as e:
        print(f"[Cache get error] {e}")
        return None

def set_cache(key, value):
    """Buffer cache writes instead of writing immediately to reduce write QPS."""
    global CACHE_BUFFER
    try:
        entry = {
            "key": key,
            "value": json.dumps(value),
        }
        CACHE_BUFFER.append(entry)
        if len(CACHE_BUFFER) >= CACHE_BUFFER_LIMIT:
            flush_cache_buffer()
    except Exception as e:
        print(f"[Cache set error] {e}")

# -----------------------------
# External API constants
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
WORLDPOP_DATASET = "wpgppop"
WORLDPOP_YEAR = 2020
WORLDPOP_TEMPLATE = "https://api.worldpop.org/v1/services/stats"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"

# -----------------------------
# DuckDB S3 connection (on-demand)
BUCKET = "s3://overturemaps-us-west-2/release/2025-10-22.0"

print("[Startup] Initializing DuckDB connection...")
conn = duckdb.connect(database=':memory:')
conn.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
conn.execute("SET s3_region='us-west-2'; SET memory_limit='4GB'; SET threads=2;")

# -----------------------------
# Cached DuckDB query function with Supabase caching
def query_duckdb_with_cache(table, type_, lat, lon, delta=0.01):
    """Step 1: Check Supabase cache, Step 2: Use DuckDB if not found, Step 3: Store result in Supabase"""
    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    cache_key = f"duckdb_{table}_{type_}_{lat_r}_{lon_r}"
    
    # Step 1: Check Supabase cache
    cached_result = get_cache(cache_key)
    if cached_result is not None:
        print(f"[Cache] Hit for {table}/{type_} at ({lat_r}, {lon_r})")
        return cached_result
    
    print(f"[Cache] Miss for {table}/{type_} at ({lat_r}, {lon_r}), querying DuckDB...")
    
    # Step 2: Use DuckDB/Overture
    path_pattern = f"{BUCKET}/theme={table}/type={type_}/*"
    query = f"""
    SELECT id,
           COALESCE(names.primary, NULL) AS name,
           ST_Distance(ST_Point({lon_r}, {lat_r})::GEOMETRY, geometry) AS distance
    FROM read_parquet('{path_pattern}', filename=True, hive_partitioning=1)
    WHERE bbox.xmin BETWEEN {lon_r - delta} AND {lon_r + delta}
      AND bbox.ymin BETWEEN {lat_r - delta} AND {lat_r + delta}
    ORDER BY distance
    LIMIT 1;
    """
    
    try:
        result = conn.execute(query).fetchone()
        if result:
            db_result = {"id": result[0], "name": result[1], "distance": float(result[2])}
            # Step 3: Store result in Supabase
            set_cache(cache_key, db_result)
            print(f"[DuckDB] Found and cached: {table}/{type_}")
            return db_result
        else:
            # Cache None results too to avoid repeated queries
            set_cache(cache_key, None)
            print(f"[DuckDB] No results found for {table}/{type_}")
            return None
    except Exception as e:
        print(f"[DuckDB query error for {table}/{type_}]: {e}")
        set_cache(cache_key, None)
        return None

# -----------------------------
# Water check with caching
def is_point_on_water_with_cache(lat, lon, delta=0.01):
    """Water check with Supabase caching"""
    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    cache_key = f"water_check_{lat_r}_{lon_r}"
    
    # Step 1: Check cache
    cached_result = get_cache(cache_key)
    if cached_result is not None:
        return cached_result
    
    # Step 2: Query DuckDB
    path = f"{BUCKET}/theme=base/type=water/*"
    query = f"""
    SELECT COUNT(*) > 0 AS on_water
    FROM read_parquet('{path}', filename=True, hive_partitioning=1)
    WHERE bbox.xmin BETWEEN {lon_r - delta} AND {lon_r + delta}
      AND bbox.ymin BETWEEN {lat_r - delta} AND {lat_r + delta}
      AND ST_Intersects(ST_Point({lon_r}, {lat_r})::GEOMETRY, geometry);
    """
    
    try:
        result = conn.execute(query).fetchone()
        on_water = bool(result[0]) if result else False
        # Step 3: Store in cache
        set_cache(cache_key, on_water)
        print(f"[Water Check] Cached result: {'on water' if on_water else 'on land'}")
        return on_water
    except Exception as e:
        print(f"[DuckDB water check error] {e}")
        set_cache(cache_key, False)
        return False

# -----------------------------
# WorldPop with caching
def point_to_geojson(lat, lon, delta=0.01):
    """Create a tiny square GeoJSON polygon around a point"""
    return {
        "type": "Polygon",
        "coordinates": [[
            [lon - delta, lat - delta],
            [lon + delta, lat - delta],
            [lon + delta, lat + delta],
            [lon - delta, lat + delta],
            [lon - delta, lat - delta]
        ]]
    }

def get_worldpop_population_with_cache(lat, lon):
    """WorldPop with Supabase caching"""
    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    cache_key = f"worldpop_{lat_r}_{lon_r}"
    
    # Step 1: Check cache
    cached_result = get_cache(cache_key)
    if cached_result is not None:
        return cached_result
    
    # Step 2: Query WorldPop API
    geojson = json.dumps({
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": point_to_geojson(lat, lon)
            }
        ]
    })

    params = {
        "dataset": WORLDPOP_DATASET,
        "year": WORLDPOP_YEAR,
        "geojson": geojson,
        "runasync": "false"
    }

    try:
        r = requests.get(WORLDPOP_TEMPLATE, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        population = data.get("data", {}).get("total_population", 0)
        result = {"population": population}
    except Exception as e:
        print(f"[WorldPop error] {e}")
        result = {"population": 0, "error": "WorldPop request failed"}
    
    # Step 3: Store in cache
    set_cache(cache_key, result)
    return result

# -----------------------------
# Nominatim with caching and rate-limiting
last_nominatim_call = 0
NOMINATIM_DELAY = 0.5

def nominatim_lookup_with_cache(lat, lon):
    """Nominatim with Supabase caching and rate limiting"""
    global last_nominatim_call
    
    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    cache_key = f"nominatim_{lat_r}_{lon_r}"
    
    # Step 1: Check cache
    cached_result = get_cache(cache_key)
    if cached_result is not None:
        return cached_result
    
    # Step 2: Query Nominatim API with rate limiting
    elapsed = time.time() - last_nominatim_call
    if elapsed < NOMINATIM_DELAY:
        time.sleep(NOMINATIM_DELAY - elapsed)

    params = {"format": "json", "lat": lat, "lon": lon, "addressdetails": 1}
    
    try:
        r = requests.get(NOMINATIM_URL, params=params,
                         headers={"User-Agent": "CoordinateChecker/1.0"}, timeout=10)
        last_nominatim_call = time.time()
        result = r.json()
    except Exception as e:
        result = {"error": str(e)}
    
    # Step 3: Store in cache
    set_cache(cache_key, result)
    return result

# -----------------------------
# Batch validation endpoint
@app.route("/api/validate_batch", methods=["POST"])
def validate_batch():
    data = request.json
    coordinates = data.get('coordinates', [])
    if not coordinates:
        return jsonify({"error": "No coordinates provided"}), 400

    def validate_single(coord):
        lat = float(coord['lat'])
        lon = float(coord['lon'])
        result = {'lat': lat, 'lon': lon, 'name': coord.get('name', 'Unknown')}

        try:
            # All queries now use cached versions
            building = query_duckdb_with_cache("buildings", "building", lat, lon)
            result['building'] = {
                'valid': building is not None,
                'distance': round(building['distance'], 2) if building else None
            }

            road = query_duckdb_with_cache("transportation", "segment", lat, lon)
            result['road'] = {
                'valid': road is not None,
                'distance': round(road['distance'], 2) if road else None
            }

            water = query_duckdb_with_cache("base", "water", lat, lon)
            result['water'] = {
                'valid': water is not None,
                'distance': round(water['distance'], 2) if water else None
            }

            place = query_duckdb_with_cache("places", "place", lat, lon)
            result['place'] = {
                'valid': place is not None,
                'distance': round(place['distance'], 2) if place else None,
                'name': place.get('name') if place else None
            }
            
            # Additional data with caching
            result['population'] = get_worldpop_population_with_cache(lat, lon)
            result['nominatim'] = nominatim_lookup_with_cache(lat, lon)
            
        except Exception as e:
            result['error'] = str(e)

        return result

    results = list(executor.map(validate_single, coordinates))
    
    # Flush any remaining cache entries
    flush_cache_buffer()
    
    return jsonify({'results': results})

# -----------------------------
# Individual endpoints (updated to use cached versions)
@app.route("/api/worldpop", methods=["GET"])
def worldpop():
    try:
        lat = float(request.args.get("lat") or request.args.get("latitude"))
        lon = float(request.args.get("lon") or request.args.get("longitude"))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid or missing coordinates"}), 400

    result = get_worldpop_population_with_cache(lat, lon)
    return jsonify(result)

@app.route("/api/nominatim", methods=["GET"])
def nominatim():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid coordinates"}), 400
    return jsonify(nominatim_lookup_with_cache(lat, lon))

@app.route("/api/building_distance", methods=["GET"])
def building_distance():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid coordinates"}), 400
        
    row = query_duckdb_with_cache("buildings", "building", lat, lon)
    if not row:
        return {"valid": False, "distance": None, "message": "No building nearby"}
    return {"valid": True, "distance": round(row["distance"], 2), "message": "Nearest building distance"}

@app.route("/api/road_distance", methods=["GET"])
def road_distance():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid coordinates"}), 400
        
    row = query_duckdb_with_cache("transportation", "segment", lat, lon)
    if not row:
        return {"valid": False, "distance": None, "message": "No road nearby"}
    return {"valid": True, "distance": round(row["distance"], 2), "message": "Nearest road distance"}

@app.route("/api/water_check", methods=["GET"])
def water_check():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid coordinates"}), 400
        
    on_water = is_point_on_water_with_cache(lat, lon)
    return {
        "on_water": on_water,
        "message": "Point lies on water" if on_water else "Point is on land"
    }

@app.route("/api/overture_match", methods=["GET"])
def overture_match():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid coordinates"}), 400
        
    row = query_duckdb_with_cache("places", "place", lat, lon)
    if not row:
        return {"valid": False, "message": "No nearby place found"}
    return {
        "valid": True,
        "message": f"Closest Overture entity: {row.get('name', 'unknown')}",
        "distance": round(float(row["distance"]), 2)
    }

# -----------------------------
# Overpass passthrough (unchanged)
@app.route("/api/overpass", methods=["POST"])
def overpass():
    try:
        query = request.data.decode("utf-8")
        if not query:
            return jsonify({"error": "Empty Overpass query"}), 400
        r = requests.post(OVERPASS_URL, data=query, timeout=60)
        r.raise_for_status()
        data = r.json()
        return jsonify({"elements": data.get("elements", [])})
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 502

# -----------------------------
# Run Flask
if __name__ == "__main__":
    # Test Supabase connection
    try:
        _ = supabase.table("cache").select("key").limit(1).execute()
        print("[Supabase] connected (cache table reachable)")
    except Exception as e:
        print(f"[Supabase] connectivity warning: {e}")

    app.run(debug=False, port=5000, threaded=True)