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
import redis  # Needed for redis_client

# -----------------------------
# Configuration and Initialization
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
# Upstash Client
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

# -----------------------------
# Redis Client (fallback or local)
redis_client = None
if USE_REDIS_FALLBACK:
    try:
        redis_client = redis.from_url(REDIS_URL)
        redis_client.ping()
        print("[Redis] client initialized")
    except Exception as e:
        print(f"[Redis] init error: {e}")
        redis_client = None

# -----------------------------
# Unpack helper
def unpack(value: str) -> any:
    try:
        if value == "__NULL__":
            return None
        compressed = base64.b64decode(value.encode())
        raw = zlib.decompress(compressed)
        return json.loads(raw.decode())
    except Exception:
        try:
            if isinstance(value, bytes):
                value = value.decode()
            return json.loads(value)
        except Exception:
            return value

# -----------------------------
# Upstash SCAN helper (tuple format)
def upstash_scan_all(prefix: str) -> set:
    keys = set()
    if not upstash_client:
        return keys

    cursor = 0
    while True:
        try:
            cursor, batch = upstash_client.scan(cursor=cursor, match=f"{prefix}*")
        except Exception as e:
            print("[Upstash scan error]", e)
            break

        for k in batch:
            if isinstance(k, bytes):
                k = k.decode()
            keys.add(k)

        if cursor == 0:
            break

    return keys

# -----------------------------
# Cleanup Function
def delete_worldpop_failures():
    wp_prefix = "water_check_"
    deleted = 0

    # -----------------------------
    # Fetch all keys from Upstash
    print("Scanning Upstash for water_check_* keys...")
    all_keys = upstash_scan_all(wp_prefix)

    # Optionally scan local/fallback Redis
    if redis_client:
        try:
            cursor = 0
            while True:
                cursor, keys = redis_client.scan(cursor, match=f"{wp_prefix}*")
                all_keys.update(k.decode() if isinstance(k, bytes) else k for k in keys)
                if cursor == 0:
                    break
        except Exception as e:
            print(f"[Redis scan error] {e}")

    print(f"Found {len(all_keys)} water_check_* keys.")

    # -----------------------------
    # Delete failed cache entries
    for key in all_keys:
        value = None

        if upstash_client:
            try:
                value = upstash_client.get(key)
            except Exception:
                pass

        # if value is None and redis_client:
        #     try:
        #         value = redis_client.get(key)
        #     except Exception:
        #         pass

        # if value is None:
            continue

        try:
            obj = unpack(value)
        except Exception:
            continue

        # if isinstance(obj, dict) and obj.get("source") == "failed":
        #     print(f"Deleting failed cache entry: {key}")

        if upstash_client:
            try:
                upstash_client.delete(key)
            except Exception:
                pass

        if redis_client:
            try:
                redis_client.delete(key)
            except Exception:
                pass

        deleted += 1

    print(f"Deleted {deleted} failed WorldPop cache entries.")

# -----------------------------
if __name__ == "__main__":
    delete_worldpop_failures()
