from flask import Flask, render_template, request, jsonify, redirect, url_for
import json
import os
import threading
import time
import heapq

app = Flask(__name__)

# ---------------- CONFIG ----------------
PRINT_SPEED = 1.0
RR_QUANTUM = 2

# ---------------- STATE ----------------
job_id = 1

STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queue_state.json")
_state_lock = threading.Lock()

fifo_queue, fifo_completed = [], []
priority_queue, priority_completed = [], []
sjf_queue, sjf_completed = [], []
ljf_queue, ljf_completed = [], []
rr_queue, rr_completed = [], []

# ---------------- JOB ----------------
class Job:
    def __init__(self, job_id, emp, doc, pages, priority=2):
        self.job_id = job_id
        self.emp = emp
        self.doc = doc
        self.pages = float(pages)
        self.remaining = float(pages)
        self.priority = priority
        self.timestamp = time.time()
        self.started_at = None
        self.status = "Queued"

    def __lt__(self, other):
        return (self.priority, self.timestamp) < (other.priority, other.timestamp)

    def to_state_dict(self):
        return {
            "job_id": self.job_id,
            "emp": self.emp,
            "doc": self.doc,
            "pages": self.pages,
            "remaining": self.remaining,
            "priority": self.priority,
            "timestamp": self.timestamp,
            "started_at": self.started_at,
            "status": self.status,
        }

    @staticmethod
    def from_state_dict(d):
        job = Job(
            int(d.get("job_id")),
            d.get("emp"),
            d.get("doc"),
            float(d.get("pages", 0)),
            int(d.get("priority", 2)),
        )
        job.remaining = float(d.get("remaining", job.pages))
        job.timestamp = float(d.get("timestamp", time.time()))
        job.started_at = d.get("started_at", None)
        job.status = d.get("status", "Queued")
        return job

# ---------------- COMMON ----------------
def progress(job, now):
    if job.status == "Completed":
        return 1.0

    if job.started_at is None:
        return (job.pages - job.remaining) / job.pages

    elapsed = now - job.started_at
    done = elapsed * PRINT_SPEED
    total_done = (job.pages - job.remaining) + done

    return min(total_done / job.pages, 1.0)

def serialize(job, now):
    return {
        "job_id": job.job_id,
        "employee": job.emp,
        "document": job.doc,
        "pages": int(job.pages),
        "priority": int(job.priority),
        "timestamp": job.timestamp,
        "status": job.status,
        "progress_percent": int(progress(job, now) * 100)
    }


def _safe_read_state_file():
    if not os.path.exists(STATE_PATH):
        return None
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                return None
            return json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return None


def load_state():
    global job_id
    global fifo_queue, fifo_completed
    global priority_queue, priority_completed
    global sjf_queue, sjf_completed
    global ljf_queue, ljf_completed
    global rr_queue, rr_completed

    with _state_lock:
        data = _safe_read_state_file()
        if not data:
            return

        job_id = int(data.get("job_id", job_id))

        fifo_queue = [Job.from_state_dict(d) for d in data.get("fifo", {}).get("queue", [])]
        fifo_completed = [Job.from_state_dict(d) for d in data.get("fifo", {}).get("completed", [])]

        priority_queue = [Job.from_state_dict(d) for d in data.get("priority", {}).get("queue", [])]
        priority_completed = [Job.from_state_dict(d) for d in data.get("priority", {}).get("completed", [])]
        heapq.heapify(priority_queue)

        sjf_queue = [Job.from_state_dict(d) for d in data.get("sjf", {}).get("queue", [])]
        sjf_completed = [Job.from_state_dict(d) for d in data.get("sjf", {}).get("completed", [])]
        heapq.heapify(sjf_queue)

        ljf_queue = [Job.from_state_dict(d) for d in data.get("ljf", {}).get("queue", [])]
        ljf_completed = [Job.from_state_dict(d) for d in data.get("ljf", {}).get("completed", [])]
        heapq.heapify(ljf_queue)

        rr_queue = [Job.from_state_dict(d) for d in data.get("rr", {}).get("queue", [])]
        rr_completed = [Job.from_state_dict(d) for d in data.get("rr", {}).get("completed", [])]


def save_state():
    with _state_lock:
        data = {
            "job_id": int(job_id),
            "fifo": {
                "queue": [j.to_state_dict() for j in fifo_queue],
                "completed": [j.to_state_dict() for j in fifo_completed],
            },
            "priority": {
                "queue": [j.to_state_dict() for j in priority_queue],
                "completed": [j.to_state_dict() for j in priority_completed],
            },
            "sjf": {
                "queue": [j.to_state_dict() for j in sjf_queue],
                "completed": [j.to_state_dict() for j in sjf_completed],
            },
            "ljf": {
                "queue": [j.to_state_dict() for j in ljf_queue],
                "completed": [j.to_state_dict() for j in ljf_completed],
            },
            "rr": {
                "queue": [j.to_state_dict() for j in rr_queue],
                "completed": [j.to_state_dict() for j in rr_completed],
            },
        }

        # Atomic-ish write: write temp then replace
        tmp_path = STATE_PATH + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, STATE_PATH)

# ---------------- PROCESSORS ----------------
def process_fifo():
    now = time.time()
    if not fifo_queue:
        return

    job = fifo_queue[0]

    if job.started_at is None:
        job.started_at = now
        job.status = "Printing"

    if progress(job, now) >= 1:
        job.status = "Completed"
        fifo_completed.append(job)
        fifo_queue.pop(0)
        process_fifo()

def process_priority():
    now = time.time()
    if not priority_queue:
        return

    job = priority_queue[0]

    if job.started_at is None:
        job.started_at = now
        job.status = "Printing"

    if progress(job, now) >= 1:
        job.status = "Completed"
        priority_completed.append(job)
        heapq.heappop(priority_queue)
        process_priority()

def process_sjf():
    now = time.time()
    if not sjf_queue:
        return

    job = sjf_queue[0]

    if job.started_at is None:
        job.started_at = now
        job.status = "Printing"

    if progress(job, now) >= 1:
        job.status = "Completed"
        sjf_completed.append(job)
        heapq.heappop(sjf_queue)
        process_sjf()

def process_ljf():
    now = time.time()
    if not ljf_queue:
        return

    job = ljf_queue[0]

    if job.started_at is None:
        job.started_at = now
        job.status = "Printing"

    if progress(job, now) >= 1:
        job.status = "Completed"
        ljf_completed.append(job)
        heapq.heappop(ljf_queue)
        process_ljf()

def process_rr():
    now = time.time()
    if not rr_queue:
        return

    job = rr_queue[0]

    if job.started_at is None:
        job.started_at = now
        job.status = "Printing"

    elapsed = now - job.started_at

    if elapsed >= RR_QUANTUM:
        job.remaining -= RR_QUANTUM

        if job.remaining <= 0:
            job.status = "Completed"
            rr_completed.append(job)
        else:
            job.status = "Queued"
            job.started_at = None
            rr_queue.append(job)

        rr_queue.pop(0)

# ---------------- PAGE ROUTES ----------------
@app.route("/")
def home():
    return redirect("/fifo")

@app.route("/fifo")
def fifo_page():
    return render_template("fifo.html")

@app.route("/priority")
def priority_page():
    return render_template("priority.html")

@app.route("/sjf")
def sjf_page():
    return render_template("sjf.html")

@app.route("/ljf")
def ljf_page():
    return render_template("ljf.html")

@app.route("/roundrobin")
def rr_page():
    return render_template("roundrobin.html")

@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "service": "queue-scheduler",
        "queues": {
            "fifo": len(fifo_queue),
            "priority": len(priority_queue),
            "sjf": len(sjf_queue),
            "ljf": len(ljf_queue),
            "round_robin": len(rr_queue)
        }
    }), 200

# ---------------- API ROUTES ----------------
@app.route("/add-fifo", methods=["POST"])
def add_fifo():
    global job_id
    d = request.json
    fifo_queue.append(Job(job_id, d["employee"], d["document"], d["pages"]))
    job_id += 1
    save_state()
    return jsonify({"msg": "ok"})

@app.route("/fifo-jobs")
def fifo_jobs():
    process_fifo()
    save_state()
    now = time.time()
    return jsonify({
        "queue": [serialize(j, now) for j in fifo_queue],
        "completed": [serialize(j, now) for j in fifo_completed]
    })

@app.route("/add-priority", methods=["POST"])
def add_priority():
    global job_id
    d = request.json
    heapq.heappush(priority_queue, Job(job_id, d["employee"], d["document"], d["pages"], int(d["priority"])))
    job_id += 1
    save_state()
    return jsonify({"msg": "ok"})

@app.route("/priority-jobs")
def priority_jobs():
    process_priority()
    save_state()
    now = time.time()
    return jsonify({
        "queue": [serialize(j, now) for j in sorted(priority_queue)],
        "completed": [serialize(j, now) for j in priority_completed]
    })

@app.route("/add-sjf", methods=["POST"])
def add_sjf():
    global job_id
    d = request.json
    heapq.heappush(sjf_queue, Job(job_id, d["employee"], d["document"], d["pages"], int(d["pages"])))
    job_id += 1
    save_state()
    return jsonify({"msg": "ok"})

@app.route("/sjf-jobs")
def sjf_jobs():
    process_sjf()
    save_state()
    now = time.time()
    return jsonify({
        "queue": [serialize(j, now) for j in sorted(sjf_queue)],
        "completed": [serialize(j, now) for j in sjf_completed]
    })

@app.route("/add-ljf", methods=["POST"])
def add_ljf():
    global job_id
    d = request.json
    heapq.heappush(ljf_queue, Job(job_id, d["employee"], d["document"], d["pages"], -int(d["pages"])))
    job_id += 1
    save_state()
    return jsonify({"msg": "ok"})

@app.route("/ljf-jobs")
def ljf_jobs():
    process_ljf()
    save_state()
    now = time.time()
    return jsonify({
        "queue": [serialize(j, now) for j in sorted(ljf_queue)],
        "completed": [serialize(j, now) for j in ljf_completed]
    })

@app.route("/add-roundrobin", methods=["POST"])
def add_rr():
    global job_id
    d = request.json
    rr_queue.append(Job(job_id, d["employee"], d["document"], d["pages"]))
    job_id += 1
    save_state()
    return jsonify({"msg": "ok"})

@app.route("/roundrobin-jobs")
def rr_jobs():
    process_rr()
    save_state()
    now = time.time()
    return jsonify({
        "queue": [serialize(j, now) for j in rr_queue],
        "completed": [serialize(j, now) for j in rr_completed]
    })


# Load persisted queues/history at startup (if present)
load_state()

if __name__ == "__main__":
    app.run(debug=True)