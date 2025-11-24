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
import redis
from contextlib import asynccontextmanager

# -----------------------------
# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
CACHE_BUFFER_LIMIT = int(os.getenv("CACHE_BUFFER_LIMIT", "50"))
USE_REDIS = os.getenv("USE_REDIS", "false").lower() == "true"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# -----------------------------
# Initialize caching layer (Redis or Supabase)
if USE_REDIS:
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        print("[Redis] Connected successfully")
    except Exception as e:
        print(f"[Redis] Connection failed: {e}. Falling back to Supabase.")
        USE_REDIS = False

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = Flask(__name__)
CORS(app)

# -----------------------------
# Optimized thread pool with more workers for I/O bound tasks
executor = ThreadPoolExecutor(max_workers=10)

# -----------------------------
# 🔥 OPTIMIZED CACHE BUFFER with immediate flushing capability
CACHE_BUFFER = []
BUFFER_LOCK = __import__('threading').Lock()

def flush_cache_buffer(force=False):
    """Write buffered cache entries to Supabase (upsert) with optimized batching."""
    global CACHE_BUFFER
    
    with BUFFER_LOCK:
        if not CACHE_BUFFER:
            return
        
        buffer_to_flush = CACHE_BUFFER.copy()
        CACHE_BUFFER = []  # Clear immediately to free memory
    
    try:
        # Batch upsert with chunking to avoid payload limits
        chunk_size = 100
        for i in range(0, len(buffer_to_flush), chunk_size):
            chunk = buffer_to_flush[i:i + chunk_size]
            supabase.table("cache").upsert(chunk).execute()
        
        print(f"[Cache] Flushed {len(buffer_to_flush)} items to Supabase")
    except Exception as e:
        print(f"[Cache flush error] {e}")

# Ensure flush at process exit
atexit.register(lambda: flush_cache_buffer(force=True))

# -----------------------------
# REDIS CACHE LAYER (if enabled)
def get_cache(key: str) -> Optional[any]:
    """Load cached value with Redis fallback to Supabase."""
    if USE_REDIS:
        try:
            cached = redis_client.get(key)
            if cached is not None:
                return json.loads(cached) if cached != "null" else None
        except Exception as e:
            print(f"[Redis get error] {e}")
    
    # Fallback to Supabase
    try:
        res = supabase.table("cache").select("value").eq("key", key).execute()
        
        if not res.data or len(res.data) == 0:
            return None
            
        cache_entry = res.data[0]
        cached_value = cache_entry.get("value")
        
        if cached_value is None:
            return None
            
        if isinstance(cached_value, str):
            try:
                parsed = json.loads(cached_value)
                # Update Redis with this value if enabled
                if USE_REDIS:
                    try:
                        redis_client.setex(key, 86400, json.dumps(parsed))  # 24h TTL
                    except:
                        pass
                return parsed
            except json.JSONDecodeError:
                if cached_value in ('"null"', "null"):
                    return None
                return cached_value
        else:
            return cached_value
            
    except Exception as e:
        print(f"[Cache get error for key {key}]: {e}")
        return None


def set_cache(key: str, value: any):
    """Buffer cache writes with Redis priority."""
    global CACHE_BUFFER
    
    # Write to Redis immediately if enabled (fast)
    if USE_REDIS:
        try:
            cache_value = json.dumps(value) if value is not None else "null"
            redis_client.setex(key, 86400, cache_value)  # 24h TTL
        except Exception as e:
            print(f"[Redis set error] {e}")
    
    # Buffer Supabase writes (slower, batch later)
    with BUFFER_LOCK:
        try:
            cache_value = json.dumps(value) if value is not None else None
            
            entry = {"key": key, "value": cache_value}
            
            # Remove duplicates
            CACHE_BUFFER = [e for e in CACHE_BUFFER if e["key"] != key]
            CACHE_BUFFER.append(entry)
            
            # Flush if buffer is full
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
# OPTIMIZED DuckDB S3 connection with partition pruning
BUCKET = "s3://overturemaps-us-west-2/release/2025-10-22.0"

print("[Startup] Initializing DuckDB connection...")
conn = duckdb.connect(database=':memory:')
conn.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
conn.execute("SET s3_region='us-west-2'; SET memory_limit='256MB'; SET threads=4;")
conn.execute("SET enable_object_cache=true;")  # Enable S3 object caching

# -----------------------------
# ASYNC OVERPASS BATCH REQUESTS
async def overpass_batch_query(queries: List[Tuple[str, dict]]) -> List[dict]:
    """
    Execute multiple Overpass queries in parallel using async.
    queries: List of (query_string, metadata) tuples
    Returns: List of results with metadata
    """
    async def fetch_single(session, query_str, metadata):
        try:
            async with session.post(
                OVERPASS_URL,
                data=query_str,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                data = await response.json()
                return {
                    'elements': data.get('elements', []),
                    'metadata': metadata,
                    'success': True
                }
        except Exception as e:
            print(f"[Overpass batch error] {e}")
            return {
                'elements': [],
                'metadata': metadata,
                'success': False,
                'error': str(e)
            }
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_single(session, q, m) for q, m in queries]
        return await asyncio.gather(*tasks)


def overpass_batch_sync(queries: List[Tuple[str, dict]]) -> List[dict]:
    """Synchronous wrapper for async batch queries."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(overpass_batch_query(queries))
    finally:
        loop.close()


# -----------------------------
# Overpass helper functions (kept for single queries)
def overpass_query(query):
    """Generic overpass POST helper."""
    try:
        r = requests.post(OVERPASS_URL, data=query, timeout=30)
        r.raise_for_status()
        return r.json().get("elements", [])
    except Exception as e:
        print(f"[Overpass request failed] {e}")
        return []


def overpass_nearest_building(lat, lon, radius=200):
    """Return the nearest building (node/way) within radius (meters)."""
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
    
    return {
        "id": e.get("id"),
        "name": e.get("tags", {}).get("name"),
        "distance": dist(e),
        "source": "overpass"
    }


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
    
    return {
        "id": best.get("id"),
        "name": best.get("tags", {}).get("name"),
        "distance": best_d,
        "source": "overpass"
    }


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
    
    return {
        "id": e.get("id"),
        "name": e.get("tags", {}).get("name"),
        "distance": dist(e),
        "source": "overpass"
    }


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


# -----------------------------
# OPTIMIZED DUCKDB QUERY with partition pruning
def query_duckdb_optimized(table, type_, lat, lon, duckdb_delta=0.01):
    """
    Optimized DuckDB query with:
    - Explicit partition filtering
    - Reduced bbox scan range
    - Limited result set
    """
    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    
    # Construct path with explicit partitions to limit S3 scans
    path_pattern = f"{BUCKET}/theme={table}/type={type_}/*"
    
    # Optimized query with tighter bbox and early limit
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
        result = conn.execute(query).fetchone()
        if result:
            return {
                "id": result[0],
                "name": result[1],
                "distance": float(result[2]),
                "source": "duckdb"
            }
        return None
    except Exception as e:
        print(f"[DuckDB query error for {table}/{type_}]: {e}")
        return None


def query_with_fallback(table, type_, lat, lon, overpass_function=None, duckdb_delta=0.01):
    """
    3-Step Process with optimized queries:
    1. Check cache (Redis -> Supabase)
    2. Try DuckDB with partition pruning
    3. Fallback to Overpass
    """
    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    
    cache_key = f"duckdb_{table}_{type_}_{lat_r}_{lon_r}"
    cached_result = get_cache(cache_key)
    
    if cached_result is not None:
        return cached_result
    
    # Try DuckDB first
    final_result = query_duckdb_optimized(table, type_, lat, lon, duckdb_delta)
    
    # Fallback to Overpass if needed
    if final_result is None and overpass_function is not None:
        try:
            final_result = overpass_function(lat, lon)
        except Exception as e:
            print(f"[Overpass error for {table}/{type_}]: {e}")
    
    # Cache result (even if None)
    set_cache(cache_key, final_result)
    
    return final_result


# -----------------------------
def is_point_on_water_with_fallback(lat, lon, delta=0.01):
    """Optimized water check with caching."""
    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    cache_key = f"water_check_{lat_r}_{lon_r}"
    
    cached_result = get_cache(cache_key)
    if cached_result is not None:
        return bool(cached_result)
    
    try:
        on_water = overpass_water_check(lat, lon)
    except Exception as e:
        print(f"[Overpass water check error] {e}")
        on_water = False
    
    set_cache(cache_key, on_water)
    return on_water


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
    """WorldPop with optimized caching."""
    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    cache_key = f"worldpop_{lat_r}_{lon_r}"
    
    cached_result = get_cache(cache_key)
    if cached_result is not None:
        return cached_result
    
    geojson = json.dumps({
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "properties": {},
            "geometry": point_to_geojson(lat, lon)
        }]
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
        result = {"population": population, "source": "worldpop"}
    except Exception as e:
        print(f"[WorldPop error] {e}")
        result = {"population": 0, "error": "WorldPop request failed", "source": "failed"}
    
    set_cache(cache_key, result)
    return result


# -----------------------------
# Nominatim with caching and rate-limiting
last_nominatim_call = 0
NOMINATIM_DELAY = 0.5


def nominatim_lookup_with_cache(lat, lon):
    """Nominatim with optimized caching and rate limiting."""
    global last_nominatim_call
    
    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    cache_key = f"nominatim_{lat_r}_{lon_r}"
    
    cached_result = get_cache(cache_key)
    if cached_result is not None:
        return cached_result
    
    elapsed = time.time() - last_nominatim_call
    if elapsed < NOMINATIM_DELAY:
        time.sleep(NOMINATIM_DELAY - elapsed)

    params = {"format": "json", "lat": lat, "lon": lon, "addressdetails": 1}
    
    try:
        r = requests.get(
            NOMINATIM_URL,
            params=params,
            headers={"User-Agent": "CoordinateChecker/1.0"},
            timeout=10
        )
        last_nominatim_call = time.time()
        result = r.json()
        result["source"] = "nominatim"
    except Exception as e:
        result = {"error": str(e), "source": "failed"}
    
    set_cache(cache_key, result)
    return result


# -----------------------------
# OPTIMIZED BATCH VALIDATION with streaming results
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
            building = query_with_fallback("buildings", "building", lat, lon, overpass_nearest_building)
            result['building'] = {
                'valid': building is not None,
                'distance': round(building['distance'], 2) if building else None,
                'source': building.get('source') if building else 'none'
            }

            road = query_with_fallback("transportation", "segment", lat, lon, overpass_nearest_road)
            result['road'] = {
                'valid': road is not None,
                'distance': round(road['distance'], 2) if road else None,
                'source': road.get('source') if road else 'none'
            }

            water = query_with_fallback("base", "water", lat, lon)
            result['water'] = {
                'valid': water is not None,
                'distance': round(water['distance'], 2) if water else None,
                'source': water.get('source') if water else 'none'
            }

            place = query_with_fallback("places", "place", lat, lon, overpass_nearest_place)
            result['place'] = {
                'valid': place is not None,
                'distance': round(place['distance'], 2) if place else None,
                'name': place.get('name') if place else None,
                'source': place.get('source') if place else 'none'
            }
            
            result['on_water'] = is_point_on_water_with_fallback(lat, lon)
            result['population'] = get_worldpop_population_with_cache(lat, lon)
            result['nominatim'] = nominatim_lookup_with_cache(lat, lon)
            
        except Exception as e:
            result['error'] = str(e)

        # Flush cache immediately after each coordinate to free memory
        flush_cache_buffer()
        
        return result

    results = list(executor.map(validate_single, coordinates))
    
    # Final flush
    flush_cache_buffer(force=True)
    
    return jsonify({'results': results})


# -----------------------------
# Individual endpoints
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
        
    row = query_with_fallback("buildings", "building", lat, lon, overpass_nearest_building)
    if not row:
        return {"valid": False, "distance": None, "message": "No building nearby", "source": "none"}
    
    distance_degrees = row["distance"]
    distance_meters = distance_degrees * 111000
    
    return {
        "valid": True,
        "distance": round(distance_meters, 2),
        "distance_degrees": round(distance_degrees, 6),
        "message": "Nearest building distance",
        "source": row.get("source", "unknown")
    }


@app.route("/api/road_distance", methods=["GET"])
def road_distance():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid coordinates"}), 400
        
    row = query_with_fallback("transportation", "segment", lat, lon, overpass_nearest_road)
    if not row:
        return {"valid": False, "distance": None, "message": "No road nearby", "source": "none"}
    
    distance_original = row["distance"]
    source = row.get("source", "unknown")
    
    if source == "overpass":
        distance_meters = distance_original
    else:
        distance_meters = distance_original * 111000
    
    return {
        "valid": True,
        "distance": round(distance_meters, 2),
        "distance_original": round(distance_original, 6),
        "message": "Nearest road distance",
        "source": source
    }


@app.route("/api/water_check", methods=["GET"])
def water_check():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid coordinates"}), 400
        
    on_water = is_point_on_water_with_fallback(lat, lon)
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
        
    row = query_with_fallback("places", "place", lat, lon, overpass_nearest_place)
    if not row:
        return {"valid": False, "message": "No nearby place found", "source": "none"}
    return {
        "valid": True,
        "message": f"Closest entity: {row.get('name', 'unknown')}",
        "distance": round(float(row["distance"]), 2),
        "source": row.get("source", "unknown")
    }


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
# Health check endpoint
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "healthy",
        "cache_backend": "redis" if USE_REDIS else "supabase",
        "buffer_size": len(CACHE_BUFFER)
    })


# -----------------------------
# Run with Gunicorn (Render) or Flask dev server locally
if __name__ == "__main__":
    # Optional: local-only connection tests
    try:
        _ = supabase.table("cache").select("key").limit(1).execute()
        print("[Supabase] connected (cache table reachable)")
    except Exception as e:
        print(f"[Supabase] connectivity warning: {e}")
    
    if USE_REDIS:
        try:
            redis_client.ping()
            print("[Redis] connection verified")
        except Exception as e:
            print(f"[Redis] connectivity warning: {e}")

    # Local development only – Render uses Gunicorn
    app.run(host="0.0.0.0", port=5000)
