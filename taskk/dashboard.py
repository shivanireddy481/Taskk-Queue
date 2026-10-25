"""
Web dashboard for the Distributed Task Queue.

Provides a real-time browser UI and a JSON API consumed by the frontend.

Run:   python dashboard.py
Open:  http://localhost:5000
"""
import json
from datetime import datetime

from flask import Flask, jsonify, render_template_string

from config import WORKER_HEARTBEAT_KEY, WORKER_TIMEOUT
from redis_client import check_redis_health, get_redis_client
from task_queue import TaskQueue

app = Flask(__name__)

# HTML Template for Dashboard
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Task Queue Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            color: #fff;
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { text-align: center; margin-bottom: 30px; font-size: 2.5em; }
        h1 span { color: #00d9ff; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .card {
            background: rgba(255,255,255,0.1);
            border-radius: 15px;
            padding: 25px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255,255,255,0.1);
            transition: transform 0.3s;
        }
        .card:hover { transform: translateY(-5px); }
        .card h3 { font-size: 0.9em; color: #888; margin-bottom: 10px; text-transform: uppercase; }
        .card .value { font-size: 3em; font-weight: bold; }
        .card.pending .value { color: #ffd93d; }
        .card.processing .value { color: #6bcb77; }
        .card.scheduled .value { color: #00d9ff; }
        .card.dead .value { color: #ff6b6b; }
        .status { display: flex; align-items: center; gap: 10px; margin-top: 10px; }
        .status-dot { width: 12px; height: 12px; border-radius: 50%; }
        .status-dot.online { background: #6bcb77; box-shadow: 0 0 10px #6bcb77; }
        .status-dot.offline { background: #ff6b6b; }
        .section { background: rgba(255,255,255,0.05); border-radius: 15px; padding: 25px; margin-bottom: 20px; }
        .section h2 { margin-bottom: 20px; color: #00d9ff; }
        .worker-list { display: grid; gap: 15px; }
        .worker {
            display: flex; justify-content: space-between; align-items: center;
            background: rgba(255,255,255,0.05); padding: 15px 20px; border-radius: 10px;
        }
        .worker-status { padding: 5px 15px; border-radius: 20px; font-size: 0.85em; }
        .worker-status.idle { background: #6bcb77; color: #000; }
        .worker-status.busy { background: #ffd93d; color: #000; }
        .worker-status.dead { background: #ff6b6b; }
        .refresh-btn {
            position: fixed; bottom: 30px; right: 30px;
            background: #00d9ff; color: #000; border: none;
            padding: 15px 25px; border-radius: 30px; cursor: pointer;
            font-weight: bold; font-size: 1em;
        }
        .refresh-btn:hover { background: #fff; }
        .empty { color: #666; font-style: italic; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .loading { animation: pulse 1.5s infinite; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 Task Queue <span>Dashboard</span></h1>
        
        <div class="grid">
            <div class="card pending">
                <h3>📋 Pending</h3>
                <div class="value" id="pending">-</div>
            </div>
            <div class="card processing">
                <h3>⚙️ Processing</h3>
                <div class="value" id="processing">-</div>
            </div>
            <div class="card scheduled">
                <h3>⏰ Scheduled</h3>
                <div class="value" id="scheduled">-</div>
            </div>
            <div class="card dead">
                <h3>💀 Dead Letter</h3>
                <div class="value" id="dead_letter">-</div>
            </div>
        </div>
        
        <div class="section">
            <h2>👷 Workers</h2>
            <div class="worker-list" id="workers">
                <p class="empty">Loading...</p>
            </div>
        </div>
        
        <div class="section">
            <h2>🔌 System Status</h2>
            <div class="status">
                <div class="status-dot" id="redis-status"></div>
                <span>Redis: <span id="redis-text">Checking...</span></span>
            </div>
        </div>
    </div>
    
    <button class="refresh-btn" onclick="loadData()">🔄 Refresh</button>
    
    <script>
        async function loadData() {
            try {
                const res = await fetch('/api/stats');
                const data = await res.json();
                
                document.getElementById('pending').textContent = data.stats.pending;
                document.getElementById('processing').textContent = data.stats.processing;
                document.getElementById('scheduled').textContent = data.stats.scheduled;
                document.getElementById('dead_letter').textContent = data.stats.dead_letter;
                
                const redisDot = document.getElementById('redis-status');
                const redisText = document.getElementById('redis-text');
                if (data.redis_healthy) {
                    redisDot.className = 'status-dot online';
                    redisText.textContent = 'Connected';
                } else {
                    redisDot.className = 'status-dot offline';
                    redisText.textContent = 'Disconnected';
                }
                
                const workerList = document.getElementById('workers');
                if (data.workers.length === 0) {
                    workerList.innerHTML = '<p class="empty">No active workers</p>';
                } else {
                    workerList.innerHTML = data.workers.map(w => `
                        <div class="worker">
                            <div>
                                <strong>${w.id}</strong>
                                <div style="font-size:0.85em;color:#888">Last seen: ${w.last_seen}s ago</div>
                            </div>
                            <span class="worker-status ${w.status}">${w.status.toUpperCase()}</span>
                        </div>
                    `).join('');
                }
            } catch (e) {
                console.error('Failed to load data:', e);
            }
        }
        
        loadData();
        setInterval(loadData, 5000);
    </script>
</body>
</html>
"""


@app.route("/", methods=["GET"])
def index():
    """Render the dashboard."""
    return render_template_string(DASHBOARD_HTML)


@app.route("/api/stats", methods=["GET"])
def api_stats():
    """JSON API: queue statistics, worker list, and operational metrics."""
    queue = TaskQueue()
    redis_client = get_redis_client()

    stats = queue.get_queue_stats()

    workers = []
    heartbeats = redis_client.hgetall(WORKER_HEARTBEAT_KEY)
    current_time = datetime.now().timestamp()

    for worker_id, data_str in heartbeats.items():
        try:
            data = json.loads(data_str)          # ← JSON instead of ast.literal_eval
            last_seen = int(current_time - data.get("timestamp", 0))

            if last_seen > WORKER_TIMEOUT:
                status = "dead"
            elif data.get("status") == "busy":
                status = "busy"
            else:
                status = "idle"

            workers.append(
                {
                    "id": worker_id,
                    "status": status,
                    "last_seen": last_seen,
                    "current_task": data.get("current_task"),
                    "stats": data.get("stats", {}),
                }
            )
        except (json.JSONDecodeError, KeyError):
            pass

    return jsonify(
        {
            "stats": stats,
            "workers": workers,
            "redis_healthy": check_redis_health(),
        }
    )


@app.route("/api/health", methods=["GET"])
def health_check():
    """Liveness / readiness probe."""
    healthy = check_redis_health()
    return jsonify({"status": "healthy" if healthy else "unhealthy", "redis": healthy})


if __name__ == "__main__":
    import os
    host = os.getenv("DASHBOARD_HOST", "127.0.0.1")
    port = int(os.getenv("DASHBOARD_PORT", 5000))
    print("=" * 50)
    print("🚀 Task Queue Dashboard")
    print(f"   Open http://{host}:{port}")
    print("=" * 50)
    app.run(host=host, port=port, debug=False)

