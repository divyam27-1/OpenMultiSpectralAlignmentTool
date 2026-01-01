# OMSPEC: Open Multispectral Alignment Tool (v1.0.0)

**OMSPEC** is a high-performance, hardware-aware orchestration engine designed for large-scale multispectral image processing. It is engineered to handle 1TB+ datasets by utilizing a native D-language scheduler that manages parallel Python worker processes.

---

## 🚀 Key Features

### 1. High-Concurrency Orchestrator
OMSPEC uses a native **D Master Process** to manage worker lifecycles. This ensures minimal overhead and maximum CPU utilization during alignment tasks.

### 2. Smart Memory Throttling
The scheduler monitors system memory usage in real-time. It dynamically pauses the spawning of new workers if memory usage exceeds the user-defined threshold in `omspec.cfg`, preventing system swap-lag or out-of-memory crashes.

### 3. Self-Healing Task Queue
Each task is tracked via a **Process Control Block (PCB)**. If a worker fails, the master process automatically retries the task (configurable) before flagging it, ensuring long-running jobs are resilient against transient errors.

### 4. Fully Portable Runtime
OMSPEC ships with an embedded **Python 3.11** interpreter and all necessary engine scripts. No global Python installation or `pip install` is required on the target machine. It just-works!

---

## 📂 Project Structure

```text
OMSPEC_v1.0.0/
├── bin/
│   ├── omspec.exe      # Master Orchestrator Binary
│   └── omspec.cfg      # Hardware & Planning Configuration
├── engine/
│   └── *.py            # Engine scripts for task processing
├── python_3_11_14/     # Embedded Python runtime
└── LICENSE.txt
```

---

## 🛠 Configuration (`omspec.cfg`)

Users can tune OMSPEC to their hardware and workflow:

- `max_memory_mb`: Maximum RAM usage in MB (e.g., 8192 for 8GB). Scheduler pauses spawning workers above this limit.
- `max_retries`: Number of times a failed chunk is retried (default: 3).
- `tick_interval_ms`: Frequency of the scheduler monitor loop in milliseconds.
- `bands`: Comma-separated list of expected spectral bands (e.g., r, g, b, nir, re).

---

## 🛣 Roadmap

**v1.0.0 (Current)**

- High-speed parallel orchestration logic.
- Runtime hardware resource accounting.
- Mock-mode validation for pipeline I/O.

**v1.1.0 (Upcoming)**

- Graceful termination with SIGINT (Ctrl+C) handling.
- Real-time telemetry dashboard showing RAM usage and worker statuses.

**v1.2.0+**

- Production-grade alignment, tiling, and testing services.

---

## 💻 Quick Start

1. Extract the ZIP archive.
2. Navigate to the `bin/` directory (or add it to PATH to use anywhere)
3. Run the tool:

```bat
omspec --target=<your_target_directory> [options]
```

Logs are generated in the `log/` directory (or current working directory in v1.0.0) detailing planning and execution phases.

---

## ⚙ CLI Arguments

| Argument      | Description                                         |
|---------------|-----------------------------------------------------|
| `--align`     | Run alignment mode (default)                        |
| `--test`      | Run testbench mode                                  |
| `--tiling`    | Perform image tiling for ML pipelines              |
| `--target`    | Target directory (default: current working directory) |
| `--depth`     | Maximum scan depth for datasets (default: 3)       |

Please edit your omspec.cfg file for higher level control of your OMSPEC tool.

Example:

```bat
omspec.exe --align --target=D:\Datasets\MyProject --depth=5
```

---

## 📖 License

OMSPEC is released under the Polyform Non Commercial License. See `LICENSE.txt` for full terms.

---

## 📝 Notes

- Currently, log files are generated in the current working directory. In future versions, logs will be written to the target directory.
- Engine scripts are editable but modifying them may break the workflow. Use caution when editing `engine/*.py`.
