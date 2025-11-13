from flask import Flask, request, jsonify
from flask_cors import CORS
import duckdb
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
import requests
import time
import json


app = Flask(__name__)
CORS(app)


# Thread pool for parallel processing
# -----------------------------
executor = ThreadPoolExecutor(max_workers=3)  # safer on 8GB RAM

# -----------------------------
# External API constants
# -----------------------------
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
WORLDPOP_DATASET = "wpgppop"
WORLDPOP_YEAR = 2020
WORLDPOP_TEMPLATE = "https://api.worldpop.org/v1/services/stats"
# -----------------------------
NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"

# -----------------------------
# DuckDB S3 connection (on-demand)
# -----------------------------
BUCKET = "s3://overturemaps-us-west-2/release/2025-10-22.0"

print("[Startup] Initializing DuckDB connection...")
conn = duckdb.connect(database=':memory:')  # Keep in memory
conn.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
conn.execute("SET s3_region='us-west-2'; SET memory_limit='4GB'; SET threads=2;")


# -----------------------------
# On-demand DuckDB query function with progress and missing file cache
# -----------------------------
missing_files_cache = set()  # keep track of missing files to skip
found_files_cache = {}       # cache successful queries per table/type/lat/lon

@lru_cache(maxsize=2000)
def query_duckdb(table, type_, lat, lon, delta=0.01):
    """Query a theme/type from S3 using wildcard, with progress logs and caching."""
    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    key = (table, type_, lat_r, lon_r)

    # Return cached result if available
    if key in found_files_cache:
        return found_files_cache[key]

    # Use wildcard path for DuckDB
    path_pattern = f"{BUCKET}/theme={table}/type={type_}/*"
    print(f"[DuckDB] Querying {table}/{type_} at ({lat_r}, {lon_r}) using {path_pattern}...")

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
            found_files_cache[key] = {"id": result[0], "name": result[1], "distance": float(result[2])}
            print(f"[DuckDB] Found result: id={result[0]}, name={result[1]}, distance={result[2]:.2f}")
            return found_files_cache[key]
        else:
            found_files_cache[key] = None
            print(f"[DuckDB] No results found for {table}/{type_}")
            return None
    except Exception as e:
        print(f"[DuckDB query error for {table}/{type_}]: {e}")
        found_files_cache[key] = None
        return None

# -----------------------------
# Water check with simple logs
# -----------------------------
@lru_cache(maxsize=2000)
def is_point_on_water(lat, lon, delta=0.01):
    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    path = f"{BUCKET}/theme=base/type=water/*"
    print(f"[DuckDB] Checking water at ({lat_r}, {lon_r}) using {path}...")
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
        print(f"[DuckDB] Point is {'on water' if on_water else 'on land'}.")
        return on_water
    except Exception as e:
        print(f"[DuckDB water check error] {e}")
        return False

# -----------------------------
# Batch validation endpoint
# -----------------------------
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
            building = query_duckdb("buildings", "building", lat, lon)
            result['building'] = {
                'valid': building is not None,
                'distance': round(building['distance'], 2) if building else None
            }

            road = query_duckdb("transportation", "segment", lat, lon)
            result['road'] = {
                'valid': road is not None,
                'distance': round(road['distance'], 2) if road else None
            }

            water = query_duckdb("base", "water", lat, lon)
            result['water'] = {
                'valid': water is not None,
                'distance': round(water['distance'], 2) if water else None
            }

            place = query_duckdb("places", "place", lat, lon)
            result['place'] = {
                'valid': place is not None,
                'distance': round(place['distance'], 2) if place else None,
                'name': place.get('name') if place else None
            }
        except Exception as e:
            result['error'] = str(e)

        return result

    results = list(executor.map(validate_single, coordinates))
    return jsonify({'results': results})

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

# -----------------------------
# WorldPop caching
# -----------------------------

# -----------------------------
# WorldPop population for ~1 km around a point
# -----------------------------

@lru_cache(maxsize=500)
@lru_cache(maxsize=500)
def get_worldpop_population(lat, lon):
    """Fetch total population around a point (~1 km square) from WorldPop API"""
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
        "dataset": WORLDPOP_DATASET,   # "wpgppop"
        "year": WORLDPOP_YEAR,         # 2000-2020
        "geojson": geojson,
        "runasync": "false"            # synchronous request
    }

    try:
        r = requests.get(WORLDPOP_TEMPLATE, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        population = data.get("data", {}).get("total_population", 0)
        return {"population": population}
    except Exception as e:
        print(f"[WorldPop error] {e}")
        return {"population": 0, "error": "WorldPop request failed"}



@app.route("/api/worldpop", methods=["GET"])
def worldpop():
    try:
        lat = float(request.args.get("lat") or request.args.get("latitude"))
        lon = float(request.args.get("lon") or request.args.get("longitude"))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid or missing coordinates"}), 400

    result = get_worldpop_population(lat, lon)
    return jsonify(result)


# -----------------------------
# Nominatim with rate-limiting
# -----------------------------
last_nominatim_call = 0
NOMINATIM_DELAY = 0.5

@app.route("/api/nominatim", methods=["GET"])
def nominatim():
    global last_nominatim_call
    elapsed = time.time() - last_nominatim_call
    if elapsed < NOMINATIM_DELAY:
        time.sleep(NOMINATIM_DELAY - elapsed)

    lat = request.args.get("lat")
    lon = request.args.get("lon")
    params = {"format": "json", "lat": lat, "lon": lon, "addressdetails": 1}

    try:
        r = requests.get(NOMINATIM_URL, params=params,
                         headers={"User-Agent": "CoordinateChecker/1.0"}, timeout=10)
        last_nominatim_call = time.time()
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 502

# -----------------------------
# Overpass passthrough
# -----------------------------
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
# Distance endpoints
# -----------------------------
@app.route("/api/building_distance", methods=["GET"])
def building_distance():
    lat = float(request.args.get("lat"))
    lon = float(request.args.get("lon"))
    row = query_duckdb("buildings", "building", lat, lon)
    if not row:
        return {"valid": False, "distance": None, "message": "No building nearby"}
    return {"valid": True, "distance": round(row["distance"], 2), "message": "Nearest building distance"}

@app.route("/api/road_distance", methods=["GET"])
def road_distance():
    lat = float(request.args.get("lat"))
    lon = float(request.args.get("lon"))
    row = query_duckdb("transportation", "segment", lat, lon)
    if not row:
        return {"valid": False, "distance": None, "message": "No road nearby"}
    return {"valid": True, "distance": round(row["distance"], 2), "message": "Nearest road distance"}

# -----------------------------
# Water check
# -----------------------------
@lru_cache(maxsize=2000)
def is_point_on_water(lat, lon, delta=0.01):
    lat_r = round(lat, 4)
    lon_r = round(lon, 4)
    query = f"""
    SELECT COUNT(*) > 0 AS on_water
    FROM read_parquet('{BUCKET}/theme=base/type=water/*', filename=True, hive_partitioning=1)
    WHERE bbox.xmin BETWEEN {lon_r - delta} AND {lon_r + delta}
      AND bbox.ymin BETWEEN {lat_r - delta} AND {lat_r + delta}
      AND ST_Intersects(ST_Point({lon_r}, {lat_r})::GEOMETRY, geometry);
    """
    try:
        result = conn.execute(query).fetchone()
        return bool(result[0]) if result else False
    except Exception as e:
        print(f"[DuckDB water check error] {e}")
        return False

@app.route("/api/water_check", methods=["GET"])
def water_check():
    lat = float(request.args.get("lat"))
    lon = float(request.args.get("lon"))
    on_water = is_point_on_water(lat, lon)
    return {
        "on_water": on_water,
        "message": "Point lies on water" if on_water else "Point is on land"
    }

# -----------------------------
# Overture place match
# -----------------------------
@app.route("/api/overture_match", methods=["GET"])
def overture_match():
    lat = float(request.args.get("lat"))
    lon = float(request.args.get("lon"))
    row = query_duckdb("places", "place", lat, lon)
    if not row:
        return {"valid": False, "message": "No nearby place found"}
    return {
        "valid": True,
        "message": f"Closest Overture entity: {row.get('name', 'unknown')}",
        "distance": round(float(row["distance"]), 2)
    }

# -----------------------------
# Run Flask
# -----------------------------
if __name__ == "__main__":
    app.run(debug=False, port=5000, threaded=True)

