from flask import Flask, request, jsonify
import requests
from flask_cors import CORS
import duckdb

app = Flask(__name__)
CORS(app)

# -----------------------------
# External API constants
# -----------------------------
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
WORLDPOP_TEMPLATE = (
    "https://api.worldpop.org/v1/services/stats?"
    "dataset=ppp_2020_1km_Aggregated&latitude={lat}&longitude={lon}&radius=1"
)
NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"

# -----------------------------
# Overture (DuckDB) setup
# -----------------------------
OVERTURE_RELEASE = "2025-10-22.0"
BUCKET = f"s3://overturemaps-us-west-2/release/{OVERTURE_RELEASE}"

def query_duckdb(theme, type_, lat, lon, delta=0.02):
    """Query Overture S3 data around given coordinates using DuckDB (no credentials needed)."""
    query = f"""
    INSTALL spatial;
    LOAD spatial;
    SELECT id,
           names.primary AS name,
           ST_Distance(ST_Point({lon}, {lat}), geometry) AS distance
    FROM read_parquet('{BUCKET}/theme={theme}/type={type_}/*',
                      filename=true, hive_partitioning=1)
    WHERE bbox.xmin BETWEEN {lon - delta} AND {lon + delta}
      AND bbox.ymin BETWEEN {lat - delta} AND {lat + delta}
    ORDER BY distance
    LIMIT 1;
    """
    try:
        df = duckdb.query(query).to_df()
        if df.empty:
            return None
        return df.iloc[0].to_dict()
    except Exception as e:
        print(f"[DuckDB error] {e}")
        return None


# -----------------------------
# 1. Overture (Healthcare fallback via Overpass)
# -----------------------------
@app.route("/api/overture", methods=["GET"])
def overture_alternative():
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
        radius = float(request.args.get("radius", 100))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid or missing lat/lon/radius"}), 400

    overpass_query = f"""
    [out:json][timeout:25];
    (
      node["amenity"~"hospital|clinic|doctors"](around:{radius},{lat},{lon});
      way["amenity"~"hospital|clinic|doctors"](around:{radius},{lat},{lon});
      relation["amenity"~"hospital|clinic|doctors"](around:{radius},{lat},{lon});
    );
    out center;
    """

    try:
        response = requests.post(OVERPASS_URL, data=overpass_query, timeout=60)
        response.raise_for_status()
        data = response.json()
        features = []
        for el in data.get("elements", []):
            if "lat" in el and "lon" in el:
                el_lat, el_lon = el["lat"], el["lon"]
            elif "center" in el:
                el_lat, el_lon = el["center"]["lat"], el["center"]["lon"]
            else:
                continue
            features.append({
                "name": el.get("tags", {}).get("name", "Unnamed"),
                "type": el.get("tags", {}).get("amenity", "unknown"),
                "lat": el_lat,
                "lon": el_lon
            })
        return jsonify({"features": features})
    except requests.exceptions.RequestException as e:
        return jsonify({"features": [], "error": str(e)}), 502


# -----------------------------
# 2. WorldPop
# -----------------------------
@app.route("/api/worldpop", methods=["GET"])
def worldpop():
    lat = request.args.get("lat") or request.args.get("latitude")
    lon = request.args.get("lon") or request.args.get("longitude")
    url = WORLDPOP_TEMPLATE.format(lat=lat, lon=lon)
    r = requests.get(url)
    try:
        return jsonify(r.json())
    except ValueError:
        return jsonify({"error": "WorldPop returned invalid JSON", "text": r.text}), 502


# -----------------------------
# 3. Nominatim (reverse geocode)
# -----------------------------
@app.route("/api/nominatim", methods=["GET"])
def nominatim():
    lat = request.args.get("lat")
    lon = request.args.get("lon")
    params = {
        "format": "json",
        "lat": lat,
        "lon": lon,
        "addressdetails": 1
    }
    r = requests.get(NOMINATIM_URL, params=params, headers={"User-Agent": "CoordinateChecker/1.0"})
    return jsonify(r.json())


# -----------------------------
# 4. Generic Overpass passthrough
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
# 5. NEW — Overture + DuckDB Distance APIs
# -----------------------------
@app.route("/api/building_distance", methods=["GET"])
def building_distance():
    lat = float(request.args.get("lat"))
    lon = float(request.args.get("lon"))
    row = query_duckdb("buildings", "building", lat, lon)
    if not row:
        return jsonify({"valid": False, "distance": None, "msg": "No building nearby"})
    return jsonify({"valid": True, "distance": round(float(row["distance"]), 2), "msg": "Nearest building distance"})


@app.route("/api/road_distance", methods=["GET"])
def road_distance():
    lat = float(request.args.get("lat"))
    lon = float(request.args.get("lon"))
    row = query_duckdb("transportation", "segment", lat, lon)
    if not row:
        return jsonify({"valid": False, "distance": None, "msg": "No road nearby"})
    return jsonify({"valid": True, "distance": round(float(row["distance"]), 2), "msg": "Nearest road distance"})

@app.route("/api/water_distance", methods=["GET"])
def water_distance():
    lat = float(request.args.get("lat"))
    lon = float(request.args.get("lon"))
    # Query Overture for water sources
    row = query_duckdb("water", "water_point", lat, lon)

    if not row:
        return jsonify({"valid": False, "distance": None, "msg": "No water source nearby"})
    
    return jsonify({
        "valid": True,
        "distance": round(float(row["distance"]), 2),
        "msg": "Nearest water source distance"
    })


@app.route("/api/overture_match", methods=["GET"])
def overture_match():
    lat = float(request.args.get("lat"))
    lon = float(request.args.get("lon"))
    row = query_duckdb("places", "place", lat, lon)
    if not row:
        return jsonify({"valid": False, "message": "No nearby place found"})
    return jsonify({
        "valid": True,
        "message": f"Closest Overture entity: {row.get('name', 'unknown')}",
        "distance": round(float(row["distance"]), 2)
    })


# -----------------------------
# Run Flask
# -----------------------------
if __name__ == "__main__":
    app.run(debug=True, port=5000)
