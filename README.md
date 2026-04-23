# 🖨️ Queue Scheduler Simulator (Flask)

A lightweight Flask-based web application that simulates a printer job queue using multiple scheduling algorithms. It visually demonstrates how different scheduling strategies affect job execution and completion.

---

## 🚀 Features

- FIFO (First In First Out)
- Priority Scheduling (lower value = higher priority)
- Shortest Job First (SJF)
- Longest Job First (LJF)
- Round Robin (time-sliced execution)
- Real-time progress tracking
- Background job execution using threads
- Persistent state using JSON storage

---

## ⚙️ How the Simulation Works

Each job contains:
- Employee name
- Document name
- Number of pages
- Priority (if applicable)
- Status (Queued / In Progress / Completed)
- Timestamps
- Progress percentage

### Key Logic:
- **1 page = 1 second of simulated print time**
- Background worker threads continuously process jobs
- UI polls backend APIs to fetch live updates
- Progress is computed dynamically

---

## 🧰 Tech Stack

- Python 3.9+
- Flask
- HTML (Jinja Templates)
- JSON (for persistence)

---

## 📦 Project Structure

```
project/
│
├── app.py                # Core Flask app + scheduling logic
├── queue_state.json     # Persistent queue state
├── templates/
│   ├── fifo.html
│   ├── priority.html
│   ├── sjf.html
│   ├── ljf.html
│   └── roundrobin.html
```

---

## 🛠️ Setup Instructions

### 1. Create Virtual Environment

#### Windows (PowerShell)
```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

#### macOS/Linux
```
python3 -m venv .venv
source .venv/bin/activate
```

---

### 2. Install Dependencies

```
pip install flask
```

---

### 3. Run the Application

```
python app.py
```

Open in browser:
```
http://127.0.0.1:5000/
```

---

## 🌐 Available Pages

| Algorithm      | URL |
|---------------|-----|
| FIFO          | http://127.0.0.1:5000/fifo |
| Priority      | http://127.0.0.1:5000/priority |
| SJF           | http://127.0.0.1:5000/sjf |
| LJF           | http://127.0.0.1:5000/ljf |
| Round Robin   | http://127.0.0.1:5000/roundrobin |

---

## 🔌 API Endpoints

### FIFO
- `POST /add-fifo`
- `GET /fifo-jobs`

### Priority
- `POST /add-priority`
- `GET /priority-jobs`

### SJF
- `POST /add-sjf`
- `GET /sjf-jobs`

### LJF
- `POST /add-ljf`
- `GET /ljf-jobs`

### Round Robin
- `POST /add-roundrobin`
- `GET /roundrobin-jobs`

---

## 📡 Example API Requests

### Add FIFO Job
```
curl -X POST http://127.0.0.1:5000/add-fifo \
-H "Content-Type: application/json" \
-d "{\"employee\":\"Alex\",\"document\":\"Report.pdf\",\"pages\":5}"
```

### Add Priority Job
```
curl -X POST http://127.0.0.1:5000/add-priority \
-H "Content-Type: application/json" \
-d "{\"employee\":\"Sam\",\"document\":\"Invoice.docx\",\"pages\":3,\"priority\":1}"
```

---

## 📊 Response Structure

```
{
  "queue": [...],
  "completed": [...],
  "overall": {
    "total_pages": number,
    "done_pages": number,
    "percent": number
  }
}
```

---

## 💾 Persistence

- Data is stored in `queue_state.json`
- Automatically updated on:
  - Job creation
  - Job execution
  - Job completion

### Reset State
1. Stop server  
2. Delete `queue_state.json`  
3. Restart server  

---

## ⚠️ Limitations

- Not production-ready (in-memory + thread-based)
- Debug mode may spawn duplicate workers
- Time simulation is approximate (sleep-based)
- No distributed queue handling

---

## 📌 Notes

- If you observe duplicate execution:
  - Disable Flask auto-reloader
- Designed for **learning + demonstration purposes**

---

## 📄 License

Add your preferred license:
- MIT
- Apache 2.0
- GPL

---

## 💡 Future Improvements (Suggested)

- Replace threads with task queue (Celery + Redis)
- Add authentication layer
- Add job cancellation & pause/resume
- WebSocket-based real-time updates (instead of polling)
- Dockerize for deployment
