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
def get_cache(key):
    """Load cached value for a given key.
    Returns:
        - None: No cache entry exists (need to do lookup)
        - Any value: Cache entry exists (could be data, None, dict, etc.)
    """
    try:
        res = supabase.table("cache").select("value").eq("key", key).execute()
        
        # Check if we got any results
        if not res.data or len(res.data) == 0:
            return None  # No cache entry at all
            
        # Get the first result
        cache_entry = res.data[0]
        cached_value = cache_entry.get("value")
        
        # If value is None in database, that means no cache entry
        if cached_value is None:
            return None
            
        # Parse JSON strings
        if isinstance(cached_value, str):
            try:
                return json.loads(cached_value)
            except json.JSONDecodeError:
                # If it's not valid JSON, check for string "null"
                if cached_value == '"null"' or cached_value == "null":
                    return None  # This is a cached "nothing found" result
                return cached_value
        else:
            # If it's already a Python object, return it directly
            # This could be None (meaning "nothing found"), a dict, etc.
            return cached_value
            
    except Exception as e:
        print(f"[Cache get error for key {key}]: {e}")
        return None  # Error means no cache entry


def set_cache(key, value):
    """Buffer cache writes instead of writing immediately to reduce write QPS."""
    global CACHE_BUFFER
    try:
        # Store proper JSON null instead of string "null"
        if value is None:
            cache_value = None
        else:
            cache_value = json.dumps(value)
            
        entry = {
            "key": key,
            "value": cache_value,
        }
        
        # Remove any existing entry with the same key to avoid duplicates
        CACHE_BUFFER = [e for e in CACHE_BUFFER if e["key"] != key]
        
        # Add the new entry
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
# Overpass helper functions
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
    # Compute simple distance approx using lat/lon
    def dist(e):
        if "center" in e:
            elat = e["center"]["lat"]; elon = e["center"]["lon"]
        else:
            elat = e.get("lat"); elon = e.get("lon")
        if elat is None or elon is None: return float("inf")
        return ((elat - lat)**2 + (elon - lon)**2)**0.5
    elems.sort(key=dist)
    e = elems[0]
    # pick lat/lon from center or point
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
    # Similar nearest pick by centroid
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

# -----------------------------
def query_with_fallback(table, type_, lat, lon, overpass_function=None, duckdb_delta=0.01):
    """
    3-Step Process using old cache format:
    1. Check Supabase cache (old format: duckdb_table_type_lat_lon)
    2. If not found, try DuckDB/Overture
    3. If DuckDB fails, try Overpass
    4. Store final result in Supabase (even if all failed)
    """
    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    
    # Step 1: Check Supabase cache using old format
    cache_key = f"duckdb_{table}_{type_}_{lat_r}_{lon_r}"
    cached_result = get_cache(cache_key)
    
    # If we got a result (even None), return it
    if cached_result is not None:
        print(f"[Cache] Hit for {table}/{type_} at ({lat_r}, {lon_r})")
        return cached_result
    
    # If we get here, it means no cache entry exists
    print(f"[Cache] Miss for {table}/{type_} at ({lat_r}, {lon_r})")
    
    # Initialize variables
    final_result = None
    source = "none"
    
    # Step 2: Try DuckDB/Overture
    path_pattern = f"{BUCKET}/theme={table}/type={type_}/*"
    query = f"""
    SELECT id,
           COALESCE(names.primary, NULL) AS name,
           ST_Distance(ST_Point({lon_r}, {lat_r})::GEOMETRY, geometry) AS distance
    FROM read_parquet('{path_pattern}', filename=True, hive_partitioning=1)
    WHERE bbox.xmin BETWEEN {lon_r - duckdb_delta} AND {lon_r + duckdb_delta}
      AND bbox.ymin BETWEEN {lat_r - duckdb_delta} AND {lat_r + duckdb_delta}
    ORDER BY distance
    LIMIT 1;
    """
    
    try:
        result = conn.execute(query).fetchone()
        if result:
            final_result = {
                "id": result[0], 
                "name": result[1], 
                "distance": float(result[2]),
                "source": "duckdb"
            }
            source = "duckdb"
            print(f"[DuckDB] Found: {table}/{type_}")
        else:
            # DuckDB found nothing
            source = "duckdb_not_found"
            print(f"[DuckDB] No results found for {table}/{type_}")
    except Exception as e:
        print(f"[DuckDB query error for {table}/{type_}]: {e}")
        source = "duckdb_error"
    
    # Step 3: If DuckDB failed or found nothing, try Overpass (if overpass function provided)
    if final_result is None and overpass_function is not None:
        print(f"[Overpass] Trying fallback for {table}/{type_}")
        try:
            overpass_result = overpass_function(lat, lon)
            if overpass_result:
                final_result = overpass_result
                source = "overpass"
                print(f"[Overpass] Found: {table}/{type_}")
            else:
                # Overpass also found nothing
                source = "overpass_not_found"
                print(f"[Overpass] No results found for {table}/{type_}")
        except Exception as e:
            print(f"[Overpass error for {table}/{type_}]: {e}")
            source = "overpass_error"
    
    # Step 4: Store final result in Supabase (using old format)
    set_cache(cache_key, final_result)
    print(f"[Cache] Stored result for {table}/{type_} from {source}: {final_result is not None}")
    
    return final_result
# -----------------------------
def is_point_on_water_with_fallback(lat, lon, delta=0.01):
    """Simplified water check - skip expensive DuckDB queries for now"""
    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    cache_key = f"water_check_{lat_r}_{lon_r}"
    
    # Step 1: Check cache first
    cached_result = get_cache(cache_key)
    if cached_result is not None:
        return bool(cached_result)
    
    # Step 2: Use Overpass only (much faster than DuckDB for water)
    print(f"[Overpass] Water check for ({lat_r}, {lon_r})")
    try:
        on_water = overpass_water_check(lat, lon)
        print(f"[Overpass] Water check: {'on water' if on_water else 'on land'}")
    except Exception as e:
        print(f"[Overpass water check error] {e}")
        on_water = False  # Default to not on water
    
    # Step 3: Cache the result
    set_cache(cache_key, on_water)
    
    return on_water
# -----------------------------
# WorldPop with caching (old format)
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
        result = {"population": population, "source": "worldpop"}
    except Exception as e:
        print(f"[WorldPop error] {e}")
        result = {"population": 0, "error": "WorldPop request failed", "source": "failed"}
    
    # Step 3: Store in cache
    set_cache(cache_key, result)
    return result

# -----------------------------
# Nominatim with caching and rate-limiting (old format)
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
        result["source"] = "nominatim"
    except Exception as e:
        result = {"error": str(e), "source": "failed"}
    
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
            # Use simplified queries with fallback
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
            
            # Enhanced water check
            result['on_water'] = is_point_on_water_with_fallback(lat, lon)
            
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
# Individual endpoints (updated to use simplified versions)
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
    
    # Convert from degrees to meters for more meaningful distances
    # Rough conversion: 1 degree ≈ 111 km at equator
    distance_degrees = row["distance"]
    distance_meters = distance_degrees * 111000  # Convert to meters
    
    return {
        "valid": True, 
        "distance": round(distance_meters, 2),  # Now in meters
        "distance_degrees": round(distance_degrees, 6),  # Keep original for reference
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
    
    # Handle different distance units based on source
    distance_original = row["distance"]
    source = row.get("source", "unknown")
    
    if source == "overpass":
        # Overpass returns distance in meters already
        distance_meters = distance_original
    else:
        # DuckDB returns distance in degrees, convert to meters
        # Rough conversion: 1 degree ≈ 111 km at equator
        distance_meters = distance_original * 111000
    
    return {
        "valid": True, 
        "distance": round(distance_meters, 2),  # Always in meters
        "distance_original": round(distance_original, 6),  # Keep original for reference
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