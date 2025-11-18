from flask import Flask, request, jsonify
from flask_cors import CORS
import duckdb
from concurrent.futures import ThreadPoolExecutor
import requests
import time
import json

app = Flask(__name__)
CORS(app)

# -----------------------------
# Thread pool for parallel processing
executor = ThreadPoolExecutor(max_workers=3)  # safe for 8GB RAM

# -----------------------------
# DuckDB persistent cache (on a volume, e.g., /data/cache.db on Render)
conn = duckdb.connect(database='/data/cache.db')  
conn.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")

# Cache table
conn.execute("""
CREATE TABLE IF NOT EXISTS duckdb_cache (
    key TEXT PRIMARY KEY,
    result JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# -----------------------------
# Helper functions for persistent cache
def get_cache(key):
    row = conn.execute("SELECT result FROM duckdb_cache WHERE key = ?", (key,)).fetchone()
    return json.loads(row[0]) if row else None

def set_cache(key, result):
    conn.execute("""
        INSERT INTO duckdb_cache(key, result)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET result=excluded.result
    """, (key, json.dumps(result)))

# -----------------------------
# Constants
BUCKET = "s3://overturemaps-us-west-2/release/2025-10-22.0"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
WORLDPOP_DATASET = "wpgppop"
WORLDPOP_YEAR = 2020
WORLDPOP_TEMPLATE = "https://api.worldpop.org/v1/services/stats"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"

# -----------------------------
# Core DuckDB query function with persistent caching
def query_duckdb(table, type_, lat, lon, delta=0.01):
    key = f"{table}_{type_}_{round(lat,4)}_{round(lon,4)}"
    cached = get_cache(key)
    if cached is not None:
        return cached

    path_pattern = f"{BUCKET}/theme={table}/type={type_}/*"
    query = f"""
    SELECT id,
           COALESCE(names.primary, NULL) AS name,
           ST_Distance(ST_Point({lon}, {lat})::GEOMETRY, geometry) AS distance
    FROM read_parquet('{path_pattern}', filename=True, hive_partitioning=1)
    WHERE bbox.xmin BETWEEN {lon - delta} AND {lon + delta}
      AND bbox.ymin BETWEEN {lat - delta} AND {lat + delta}
    ORDER BY distance
    LIMIT 1;
    """
    try:
        result = conn.execute(query).fetchone()
        if result:
            obj = {"id": result[0], "name": result[1], "distance": float(result[2])}
        else:
            obj = None
        set_cache(key, obj)
        return obj
    except Exception as e:
        print(f"[DuckDB error] {e}")
        set_cache(key, None)
        return None

# -----------------------------
# Water check
def is_point_on_water(lat, lon, delta=0.01):
    key = f"water_{round(lat,4)}_{round(lon,4)}"
    cached = get_cache(key)
    if cached is not None:
        return cached
    query = f"""
    SELECT COUNT(*) > 0 AS on_water
    FROM read_parquet('{BUCKET}/theme=base/type=water/*', filename=True, hive_partitioning=1)
    WHERE bbox.xmin BETWEEN {lon - delta} AND {lon + delta}
      AND bbox.ymin BETWEEN {lat - delta} AND {lat + delta}
      AND ST_Intersects(ST_Point({lon}, {lat})::GEOMETRY, geometry);
    """
    try:
        result = conn.execute(query).fetchone()
        on_water = bool(result[0]) if result else False
        set_cache(key, on_water)
        return on_water
    except Exception as e:
        print(f"[DuckDB water check error] {e}")
        set_cache(key, False)
        return False

# -----------------------------
# WorldPop population (~1km)
def point_to_geojson(lat, lon, delta=0.01):
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

def get_worldpop_population(lat, lon):
    key = f"worldpop_{round(lat,4)}_{round(lon,4)}"
    cached = get_cache(key)
    if cached:
        return cached

    geojson = json.dumps({"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":point_to_geojson(lat, lon)}]})
    params = {"dataset": WORLDPOP_DATASET, "year": WORLDPOP_YEAR, "geojson": geojson, "runasync":"false"}

    try:
        r = requests.get(WORLDPOP_TEMPLATE, params=params, timeout=30)
        r.raise_for_status()
        population = r.json().get("data", {}).get("total_population", 0)
    except Exception as e:
        print(f"[WorldPop error] {e}")
        population = 0

    set_cache(key, {"population": population})
    return {"population": population}

# -----------------------------
# Nominatim reverse geocode with persistent caching
def nominatim_lookup(lat, lon):
    key = f"nominatim_{round(lat,4)}_{round(lon,4)}"
    cached = get_cache(key)
    if cached:
        return cached
    try:
        r = requests.get(NOMINATIM_URL, params={"format":"json","lat":lat,"lon":lon,"addressdetails":1},
                         headers={"User-Agent":"CoordinateChecker/1.0"}, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[Nominatim error] {e}")
        data = {}
    set_cache(key, data)
    return data

# -----------------------------
# Flask endpoints
@app.route("/api/validate_batch", methods=["POST"])
def validate_batch():
    data = request.json
    coordinates = data.get('coordinates', [])
    if not coordinates:
        return jsonify({"error": "No coordinates provided"}), 400

    def validate_single(coord):
        lat = float(coord['lat'])
        lon = float(coord['lon'])
        result = {"lat": lat, "lon": lon, "name": coord.get('name', 'Unknown')}

        result["building"] = query_duckdb("buildings", "building", lat, lon)
        result["road"] = query_duckdb("transportation", "segment", lat, lon)
        result["water"] = is_point_on_water(lat, lon)
        result["place"] = query_duckdb("places", "place", lat, lon)
        result["population"] = get_worldpop_population(lat, lon)

        return result

    results = list(executor.map(validate_single, coordinates))
    return jsonify({"results": results})

@app.route("/api/worldpop", methods=["GET"])
def worldpop():
    try:
        lat = float(request.args.get("lat") or request.args.get("latitude"))
        lon = float(request.args.get("lon") or request.args.get("longitude"))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid or missing coordinates"}), 400

    return jsonify(get_worldpop_population(lat, lon))

@app.route("/api/nominatim", methods=["GET"])
def nominatim():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid coordinates"}), 400

    return jsonify(nominatim_lookup(lat, lon))

@app.route("/api/building_distance", methods=["GET"])
def building_distance():
    lat = float(request.args.get("lat"))
    lon = float(request.args.get("lon"))
    row = query_duckdb("buildings", "building", lat, lon)
    return {"valid": row is not None, "distance": round(row["distance"], 2) if row else None}

@app.route("/api/road_distance", methods=["GET"])
def road_distance():
    lat = float(request.args.get("lat"))
    lon = float(request.args.get("lon"))
    row = query_duckdb("transportation", "segment", lat, lon)
    return {"valid": row is not None, "distance": round(row["distance"], 2) if row else None}

@app.route("/api/water_check", methods=["GET"])
def water_check():
    lat = float(request.args.get("lat"))
    lon = float(request.args.get("lon"))
    on_water = is_point_on_water(lat, lon)
    return {"on_water": on_water, "message": "Point is on water" if on_water else "Point is on land"}

@app.route("/api/overture_match", methods=["GET"])
def overture_match():
    lat = float(request.args.get("lat"))
    lon = float(request.args.get("lon"))
    row = query_duckdb("places", "place", lat, lon)
    return {"valid": row is not None, "distance": round(row["distance"], 2) if row else None, "name": row.get("name") if row else None}

@app.route("/api/overpass", methods=["POST"])
def overpass():
    try:
        query = request.data.decode("utf-8")
        r = requests.post(OVERPASS_URL, data=query, timeout=60)
        r.raise_for_status()
        return jsonify({"elements": r.json().get("elements", [])})
    except Exception as e:
        return jsonify({"error": str(e)}), 502

if __name__ == "__main__":
    app.run(debug=False, port=5000, threaded=True)
