# gunicorn_config.py
# Production-ready Gunicorn configuration

import multiprocessing
import os

# Server socket
bind = f"0.0.0.0:{os.getenv('PORT', '5000')}"
backlog = 2048

# Worker processes
workers = int(os.getenv('GUNICORN_WORKERS', multiprocessing.cpu_count() * 2 + 1))
worker_class = 'gthread'  # Use threads for I/O-bound operations
threads = int(os.getenv('GUNICORN_THREADS', 4))
worker_connections = 1000
max_requests = 1000  # Restart workers after this many requests
max_requests_jitter = 50  # Add randomness to max_requests
timeout = 120  # Worker timeout for long-running queries
keepalive = 5

# Logging
accesslog = '-'  # Log to stdout
errorlog = '-'   # Log to stderr
loglevel = os.getenv('LOG_LEVEL', 'info')
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Process naming
proc_name = 'coordinate_validator'

# Server mechanics
daemon = False
pidfile = None
umask = 0
user = None
group = None
tmp_upload_dir = None

# Enable preload for faster worker spawning (but be careful with in-memory state)
preload_app = False  # Set to False to avoid issues with DuckDB connection sharing

# Hooks for graceful shutdown and cache flushing
def on_exit(server):
    """Flush cache buffer on shutdown."""
    print("[Gunicorn] Shutting down, flushing cache...")
    from app import flush_cache_buffer
    flush_cache_buffer(force=True)

def post_fork(server, worker):
    """Reset DuckDB connection per worker to avoid shared connections."""
    print(f"[Gunicorn] Worker {worker.pid} spawned")

def worker_exit(server, worker):
    """Flush cache when worker exits."""
    print(f"[Gunicorn] Worker {worker.pid} exiting, flushing cache...")
    from app import flush_cache_buffer
    flush_cache_buffer(force=True)