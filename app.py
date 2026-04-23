from flask import Flask, render_template, request, jsonify, redirect, url_for
import heapq
import time
import threading
import json
from pathlib import Path

app = Flask(__name__)


@app.route("/")
def index():
    return redirect(url_for("fifo_page"))

# ---------------- FIFO QUEUE ----------------
fifo_queue = []
fifo_completed = []
fifo_in_progress = None

# ---------------- PRIORITY QUEUE ----------------
priority_queue = []
priority_completed = []
priority_in_progress = None

# ---------------- SJF QUEUE (Shortest Job First) ----------------
sjf_queue = []
sjf_completed = []
sjf_in_progress = None

# ---------------- LJF QUEUE (Longest Job First) ----------------
ljf_queue = []
ljf_completed = []
ljf_in_progress = None

# ---------------- ROUND ROBIN QUEUE ----------------
rr_queue = []
rr_completed = []
rr_in_progress = None
RR_QUANTUM_SECONDS = 2

job_id = 1

STATE_PATH = Path(__file__).with_name("queue_state.json")
state_lock = threading.Lock()

class PrintJob:
    def __init__(self, job_id, employee, document, pages, priority=2):
        self.job_id = job_id
        self.employee = employee
        self.document = document
        self.pages = pages
        self.remaining_pages = pages
        self.priority = priority
        self.timestamp = time.time()
        self.started_at = None
        self.completed_at = None
        self.status = "Queued"

    def __lt__(self, other):
        return (self.priority, self.timestamp) < (other.priority, other.timestamp)


def _job_to_dict(job: PrintJob) -> dict:
    return {
        "job_id": job.job_id,
        "employee": job.employee,
        "document": job.document,
        "pages": job.pages,
        "remaining_pages": getattr(job, "remaining_pages", job.pages),
        "priority": job.priority,
        "timestamp": job.timestamp,
        "started_at": getattr(job, "started_at", None),
        "completed_at": getattr(job, "completed_at", None),
        "status": job.status,
    }


def _job_from_dict(data: dict) -> PrintJob:
    job = PrintJob(
        int(data["job_id"]),
        data.get("employee", ""),
        data.get("document", ""),
        int(data.get("pages", 0)),
        int(data.get("priority", 2)),
    )
    job.remaining_pages = float(data.get("remaining_pages", job.pages))
    job.timestamp = float(data.get("timestamp", time.time()))
    job.started_at = data.get("started_at", None)
    job.completed_at = data.get("completed_at", None)
    job.status = data.get("status", "Queued")
    return job


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def job_progress_fraction(job: PrintJob, now: float) -> float:
    pages = float(job.pages) if float(job.pages) > 0 else 1.0

    if job.status == "Completed":
        return 1.0

    if job.status == "Queued":
        # Round Robin jobs may already have partial progress even if queued.
        remaining = float(getattr(job, "remaining_pages", pages))
        printed = pages - remaining
        return _clamp(printed / pages, 0.0, 1.0)

    if job.status == "Printing":
        remaining = float(getattr(job, "remaining_pages", pages))
        printed_base = pages - remaining
        started_at = getattr(job, "started_at", None)
        if started_at is not None:
            try:
                elapsed = float(now) - float(started_at)
            except Exception:
                elapsed = 0.0
        else:
            elapsed = 0.0

        # For FIFO/Priority/SJF/LJF: remaining doesn't change while sleeping, but elapsed approximates progress.
        # For Round Robin: remaining changes in slices; elapsed gives smooth progress inside the current slice.
        printed = printed_base + max(0.0, elapsed)
        return _clamp(printed / pages, 0.0, 1.0)

    return 0.0


def _serialize_jobs_with_progress(jobs: list[PrintJob], now: float) -> list[dict]:
    result = []
    for job in jobs:
        data = _job_to_dict(job)
        frac = job_progress_fraction(job, now)
        data["progress"] = frac
        data["progress_percent"] = int(round(frac * 100))
        result.append(data)
    return result


def _overall_progress(jobs: list[PrintJob], now: float) -> dict:
    total_pages = 0.0
    done_pages = 0.0
    for job in jobs:
        pages = float(job.pages) if float(job.pages) > 0 else 0.0
        total_pages += pages
        done_pages += pages * job_progress_fraction(job, now)

    percent = 0
    if total_pages > 0:
        percent = int(round((done_pages / total_pages) * 100))

    return {
        "total_pages": total_pages,
        "done_pages": done_pages,
        "percent": percent,
    }


def save_state() -> None:
    with state_lock:
        state = {
            "job_id": job_id,
            "fifo": {
                "queue": [_job_to_dict(j) for j in fifo_queue],
                "completed": [_job_to_dict(j) for j in fifo_completed],
                "in_progress": _job_to_dict(fifo_in_progress) if fifo_in_progress else None,
            },
            "priority": {
                "queue": [_job_to_dict(j) for j in list(priority_queue)],
                "completed": [_job_to_dict(j) for j in priority_completed],
                "in_progress": _job_to_dict(priority_in_progress) if priority_in_progress else None,
            },
            "sjf": {
                "queue": [_job_to_dict(j) for j in list(sjf_queue)],
                "completed": [_job_to_dict(j) for j in sjf_completed],
                "in_progress": _job_to_dict(sjf_in_progress) if sjf_in_progress else None,
            },
            "ljf": {
                "queue": [_job_to_dict(j) for j in list(ljf_queue)],
                "completed": [_job_to_dict(j) for j in ljf_completed],
                "in_progress": _job_to_dict(ljf_in_progress) if ljf_in_progress else None,
            },
            "round_robin": {
                "queue": [_job_to_dict(j) for j in rr_queue],
                "completed": [_job_to_dict(j) for j in rr_completed],
                "in_progress": _job_to_dict(rr_in_progress) if rr_in_progress else None,
                "quantum_seconds": RR_QUANTUM_SECONDS,
            },
        }

    tmp_path = STATE_PATH.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp_path.replace(STATE_PATH)


def load_state() -> None:
    global job_id, fifo_queue, fifo_completed, fifo_in_progress
    global priority_queue, priority_completed, priority_in_progress
    global sjf_queue, sjf_completed, sjf_in_progress
    global ljf_queue, ljf_completed, ljf_in_progress
    global rr_queue, rr_completed, rr_in_progress

    if not STATE_PATH.exists():
        return

    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return

    with state_lock:
        job_id = int(state.get("job_id", job_id))

        fifo_state = state.get("fifo", {})
        fifo_queue = [_job_from_dict(j) for j in fifo_state.get("queue", [])]
        fifo_completed = [_job_from_dict(j) for j in fifo_state.get("completed", [])]
        fifo_in_progress = (
            _job_from_dict(fifo_state["in_progress"]) if fifo_state.get("in_progress") else None
        )

        priority_state = state.get("priority", {})
        priority_queue = []
        for j in priority_state.get("queue", []):
            heapq.heappush(priority_queue, _job_from_dict(j))
        priority_completed = [_job_from_dict(j) for j in priority_state.get("completed", [])]
        priority_in_progress = (
            _job_from_dict(priority_state["in_progress"]) if priority_state.get("in_progress") else None
        )

        sjf_state = state.get("sjf", {})
        sjf_queue = []
        for j in sjf_state.get("queue", []):
            heapq.heappush(sjf_queue, _job_from_dict(j))
        sjf_completed = [_job_from_dict(j) for j in sjf_state.get("completed", [])]
        sjf_in_progress = (
            _job_from_dict(sjf_state["in_progress"]) if sjf_state.get("in_progress") else None
        )

        ljf_state = state.get("ljf", {})
        ljf_queue = []
        for j in ljf_state.get("queue", []):
            heapq.heappush(ljf_queue, _job_from_dict(j))
        ljf_completed = [_job_from_dict(j) for j in ljf_state.get("completed", [])]
        ljf_in_progress = (
            _job_from_dict(ljf_state["in_progress"]) if ljf_state.get("in_progress") else None
        )

        rr_state = state.get("round_robin", {})
        rr_queue = [_job_from_dict(j) for j in rr_state.get("queue", [])]
        rr_completed = [_job_from_dict(j) for j in rr_state.get("completed", [])]
        rr_in_progress = (
            _job_from_dict(rr_state["in_progress"]) if rr_state.get("in_progress") else None
        )


# ---------------- FIFO ROUTES ----------------
@app.route("/fifo")
def fifo_page():
    return render_template("fifo.html")

@app.route("/add-fifo", methods=["POST"])
def add_fifo():
    global job_id
    data = request.json

    with state_lock:
        job = PrintJob(job_id, data["employee"], data["document"], data["pages"])
        fifo_queue.append(job)
        job_id += 1

    save_state()

    return jsonify({"msg": "Added to FIFO"})

@app.route("/fifo-jobs")
def fifo_jobs():
    now = time.time()
    with state_lock:
        queue_jobs = list(fifo_queue)
        if fifo_in_progress is not None:
            queue_jobs = [fifo_in_progress] + queue_jobs
        completed_jobs = list(fifo_completed)

        all_jobs = queue_jobs + completed_jobs

    return jsonify({
        "queue": _serialize_jobs_with_progress(queue_jobs, now),
        "completed": _serialize_jobs_with_progress(completed_jobs, now),
        "overall": _overall_progress(all_jobs, now),
    })


# ---------------- PRIORITY ROUTES ----------------
@app.route("/priority")
def priority_page():
    return render_template("priority.html")

@app.route("/add-priority", methods=["POST"])
def add_priority():
    global job_id
    data = request.json

    job = PrintJob(
        job_id,
        data["employee"],
        data["document"],
        data["pages"],
        int(data["priority"])
    )

    with state_lock:
        heapq.heappush(priority_queue, job)
        job_id += 1

    save_state()

    return jsonify({"msg": "Added to Priority Queue"})

@app.route("/priority-jobs")
def priority_jobs():
    now = time.time()
    with state_lock:
        queue_jobs = sorted(priority_queue)
        if priority_in_progress is not None:
            queue_jobs = [priority_in_progress] + queue_jobs
        completed_jobs = list(priority_completed)

        all_jobs = queue_jobs + completed_jobs

    return jsonify({
        "queue": _serialize_jobs_with_progress(queue_jobs, now),
        "completed": _serialize_jobs_with_progress(completed_jobs, now),
        "overall": _overall_progress(all_jobs, now),
    })


# ---------------- SJF ROUTES ----------------
@app.route("/sjf")
def sjf_page():
    return render_template("sjf.html")


@app.route("/add-sjf", methods=["POST"])
def add_sjf():
    global job_id
    data = request.json

    with state_lock:
        job = PrintJob(job_id, data["employee"], data["document"], data["pages"], priority=int(data["pages"]))
        heapq.heappush(sjf_queue, job)
        job_id += 1

    save_state()
    return jsonify({"msg": "Added to SJF"})


@app.route("/sjf-jobs")
def sjf_jobs():
    now = time.time()
    with state_lock:
        queue_jobs = sorted(sjf_queue)
        if sjf_in_progress is not None:
            queue_jobs = [sjf_in_progress] + queue_jobs
        completed_jobs = list(sjf_completed)

        all_jobs = queue_jobs + completed_jobs

    return jsonify({
        "queue": _serialize_jobs_with_progress(queue_jobs, now),
        "completed": _serialize_jobs_with_progress(completed_jobs, now),
        "overall": _overall_progress(all_jobs, now),
    })


# ---------------- LJF ROUTES ----------------
@app.route("/ljf")
def ljf_page():
    return render_template("ljf.html")


@app.route("/add-ljf", methods=["POST"])
def add_ljf():
    global job_id
    data = request.json

    with state_lock:
        job = PrintJob(job_id, data["employee"], data["document"], data["pages"], priority=-int(data["pages"]))
        heapq.heappush(ljf_queue, job)
        job_id += 1

    save_state()
    return jsonify({"msg": "Added to LJF"})


@app.route("/ljf-jobs")
def ljf_jobs():
    now = time.time()
    with state_lock:
        queue_jobs = sorted(ljf_queue)
        if ljf_in_progress is not None:
            queue_jobs = [ljf_in_progress] + queue_jobs
        completed_jobs = list(ljf_completed)

        all_jobs = queue_jobs + completed_jobs

    return jsonify({
        "queue": _serialize_jobs_with_progress(queue_jobs, now),
        "completed": _serialize_jobs_with_progress(completed_jobs, now),
        "overall": _overall_progress(all_jobs, now),
    })


# ---------------- ROUND ROBIN ROUTES ----------------
@app.route("/roundrobin")
def roundrobin_page():
    return render_template("roundrobin.html")


@app.route("/add-roundrobin", methods=["POST"])
def add_roundrobin():
    global job_id
    data = request.json

    with state_lock:
        job = PrintJob(job_id, data["employee"], data["document"], data["pages"])
        job.remaining_pages = float(job.pages)
        rr_queue.append(job)
        job_id += 1

    save_state()
    return jsonify({"msg": "Added to Round Robin"})


@app.route("/roundrobin-jobs")
def roundrobin_jobs():
    now = time.time()
    with state_lock:
        queue_jobs = list(rr_queue)
        if rr_in_progress is not None:
            queue_jobs = [rr_in_progress] + queue_jobs
        completed_jobs = list(rr_completed)

        all_jobs = queue_jobs + completed_jobs

    return jsonify({
        "queue": _serialize_jobs_with_progress(queue_jobs, now),
        "completed": _serialize_jobs_with_progress(completed_jobs, now),
        "overall": _overall_progress(all_jobs, now),
        "quantum_seconds": RR_QUANTUM_SECONDS,
    })


# ---------------- PRINTER WORKERS ----------------
def fifo_worker():
    global fifo_in_progress
    while True:
        job = None
        should_save = False
        with state_lock:
            if fifo_in_progress is None and fifo_queue:
                job = fifo_queue.pop(0)
                fifo_in_progress = job
                job.status = "Printing"
                job.started_at = time.time()
                should_save = True

        if should_save:
            save_state()

        if job is not None:
            print(f"[FIFO] Printing {job.document}")
            time.sleep(job.pages)
            should_save = False
            with state_lock:
                job.status = "Completed"
                job.remaining_pages = 0
                job.completed_at = time.time()
                fifo_completed.append(job)
                fifo_in_progress = None
                if len(fifo_completed) > 50:
                    del fifo_completed[:-50]
                should_save = True

            if should_save:
                save_state()
        else:
            time.sleep(2)

def priority_worker():
    global priority_in_progress
    while True:
        job = None
        should_save = False
        with state_lock:
            if priority_in_progress is None and priority_queue:
                job = heapq.heappop(priority_queue)
                priority_in_progress = job
                job.status = "Printing"
                job.started_at = time.time()
                should_save = True

        if should_save:
            save_state()

        if job is not None:
            print(f"[PRIORITY] Printing {job.document}")
            time.sleep(job.pages)
            should_save = False
            with state_lock:
                job.status = "Completed"
                job.remaining_pages = 0
                job.completed_at = time.time()
                priority_completed.append(job)
                priority_in_progress = None
                if len(priority_completed) > 50:
                    del priority_completed[:-50]
                should_save = True

            if should_save:
                save_state()
        else:
            time.sleep(2)


def sjf_worker():
    global sjf_in_progress
    while True:
        job = None
        should_save = False
        with state_lock:
            if sjf_in_progress is None and sjf_queue:
                job = heapq.heappop(sjf_queue)
                sjf_in_progress = job
                job.status = "Printing"
                job.started_at = time.time()
                should_save = True

        if should_save:
            save_state()

        if job is not None:
            print(f"[SJF] Printing {job.document}")
            time.sleep(job.pages)
            with state_lock:
                job.status = "Completed"
                job.remaining_pages = 0
                job.completed_at = time.time()
                sjf_completed.append(job)
                sjf_in_progress = None
                if len(sjf_completed) > 50:
                    del sjf_completed[:-50]
                should_save = True

            if should_save:
                save_state()
        else:
            time.sleep(2)


def ljf_worker():
    global ljf_in_progress
    while True:
        job = None
        should_save = False
        with state_lock:
            if ljf_in_progress is None and ljf_queue:
                job = heapq.heappop(ljf_queue)
                ljf_in_progress = job
                job.status = "Printing"
                job.started_at = time.time()
                should_save = True

        if should_save:
            save_state()

        if job is not None:
            print(f"[LJF] Printing {job.document}")
            time.sleep(job.pages)
            with state_lock:
                job.status = "Completed"
                job.remaining_pages = 0
                job.completed_at = time.time()
                ljf_completed.append(job)
                ljf_in_progress = None
                if len(ljf_completed) > 50:
                    del ljf_completed[:-50]
                should_save = True

            if should_save:
                save_state()
        else:
            time.sleep(2)


def roundrobin_worker():
    global rr_in_progress
    while True:
        job = None
        should_save = False
        with state_lock:
            if rr_in_progress is None and rr_queue:
                job = rr_queue.pop(0)
                rr_in_progress = job
                job.status = "Printing"
                job.started_at = time.time()
                should_save = True

        if should_save:
            save_state()

        if job is not None:
            slice_time = min(float(RR_QUANTUM_SECONDS), float(getattr(job, "remaining_pages", job.pages)))
            print(f"[RR] Printing {job.document} for {slice_time}s")
            time.sleep(slice_time)

            should_save = False
            with state_lock:
                job.remaining_pages = float(getattr(job, "remaining_pages", job.pages)) - slice_time
                if job.remaining_pages <= 0:
                    job.status = "Completed"
                    job.remaining_pages = 0
                    job.completed_at = time.time()
                    rr_completed.append(job)
                    if len(rr_completed) > 50:
                        del rr_completed[:-50]
                else:
                    job.status = "Queued"
                    job.started_at = None
                    rr_queue.append(job)
                rr_in_progress = None
                should_save = True

            if should_save:
                save_state()
        else:
            time.sleep(2)

load_state()

threading.Thread(target=fifo_worker, daemon=True).start()
threading.Thread(target=priority_worker, daemon=True).start()
threading.Thread(target=sjf_worker, daemon=True).start()
threading.Thread(target=ljf_worker, daemon=True).start()
threading.Thread(target=roundrobin_worker, daemon=True).start()

if __name__ == "__main__":
    app.run(debug=True)