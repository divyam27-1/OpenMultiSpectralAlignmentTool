module process_control;

import core.thread;
import core.time;
import core.atomic : atomicOp;

import std.stdio;
import std.process;
import std.json;
import std.file;
import std.conv : to;
import std.array;
import std.algorithm;
import std.container.dlist;
import std.math : pow;
import std.path;
import std.datetime.systime : SysTime, Clock;
import std.logger;
import std.exception : enforce;

import appstate;
import config;

SysTime current_time;
string log_filename;
FileLogger fileLogger;

// TODO: Make file logger not generate at module load time instead generate using an initLogger() method called from main only after targetPath is set
static this()
{
    if (!exists("log"))
        mkdir("log");
    current_time = Clock.currTime();
    log_filename = buildPath(targetPath, "log",
        "processing_" ~ current_time.toISOExtString().replace(":", "-") ~ ".log");
    fileLogger = new FileLogger(log_filename, LogLevel.info, CreateFolder.yes);
}

enum TaskMode
{
    MOCK,
    ALIGN,
    TEST,
    TILING
}

struct ProcessResult
{
    size_t chunk_id;
    bool success;
    int exitCode;
}

struct ProcessControlBlock
{
    Pid pid;
    size_t chunk_id;
    string json_path;
    SysTime start_time;
    int attempt;
}

public class Scheduler
{
    private ProcessRunner runner;
    private string planPath;

    private DList!size_t taskQueue;
    private ProcessControlBlock[Pid] pcbMap;

    // initialized to default values just in case something goes wrong with reading cfg
    private size_t memoryUsageMB = 0;
    private size_t maxMemoryMB = 2048;
    private int maxRetries = 3;
    private int tickIntervalMS = 75;

    this(ProcessRunner runner, string planPath, size_t maxMemoryMB = 2048)
    {
        this.runner = runner;
        this.planPath = planPath;
        this.maxMemoryMB = maxMemoryMB;

        auto cfg = loadConfig(buildPath(thisExePath().dirName, "omspec.cfg").absolutePath());
        this.maxRetries = cfg.max_retries;
        this.tickIntervalMS = cfg.tick_interval_ms;
        this.maxMemoryMB = cfg.max_memory_mb;
    }

    public void execute_plan()
    {
        // Read plan from JSON
        auto plan = parseJSON(readText(this.planPath));
        size_t num_chunks = plan.array.length;
        size_t[] chunk_sizes = plan.array.map!(c => cast(size_t) c["chunk_size"].get!long).array;
        foreach (i; 0 .. num_chunks)
        {
            this.taskQueue.insertBack(i);
        }

        while (!this.taskQueue.empty || this.pcbMap.length > 0)
        {

            // Phase 1 : Worker Spawning
            while (!this.taskQueue.empty && this.memoryUsageMB < this.maxMemoryMB)
            {
                size_t next_chunk_id = this.taskQueue.front;
                this.taskQueue.removeFront();

                Pid worker_pid = this.runner.spawn_worker(this.planPath, next_chunk_id);
                if (worker_pid is Pid.init)
                {
                    fileLogger.errorf("Failed to spawn worker for Chunk %d. Skipping", next_chunk_id);
                    continue;
                }

                ProcessControlBlock worker_pcb = ProcessControlBlock();

                worker_pcb.pid = worker_pid;
                worker_pcb.chunk_id = next_chunk_id;
                worker_pcb.json_path = this.planPath;
                worker_pcb.start_time = Clock.currTime();
                worker_pcb.attempt = 0;

                this.pcbMap[worker_pid] = worker_pcb;
                this.memoryUsageMB += chunk_sizes[next_chunk_id]; // increase memory usage

                fileLogger.infof("Spawned Worker PID %s for Chunk %d. Current Memory Usage: %d MB",
                    worker_pid.processID, next_chunk_id, this.memoryUsageMB);
            }

            // Phase 2 : Monitor Workers
            Pid[] completed_pids = [];
            Pid[] failed_pids = [];
            Pid[] retry_pids = [];
            foreach (pid, pcb; this.pcbMap)
            {
                auto result = tryWait(pid);

                if (result.terminated)
                {
                    switch (result.status)
                    {
                    case 0:
                        completed_pids ~= pid;
                        break;
                    case 1:
                        failed_pids ~= pid;
                        break;
                    case 2:
                        retry_pids ~= pid;
                        break;
                    default:
                        fileLogger.errorf("Worker PID %s for Chunk %d exited with unknown code %d",
                            pid.processID, pcb.chunk_id, result.status);
                        failed_pids ~= pid;
                        break;
                    }
                }
            }

            // Phase 3: Cleanup and Retry
            foreach (pid; completed_pids)
            {
                fileLogger.infof("Worker PID %s for Chunk %d completed successfully.",
                    pid.processID, this.pcbMap[pid].chunk_id);

                this.memoryUsageMB -= chunk_sizes[this.pcbMap[pid].chunk_id]; // decrease memory usage
                this.pcbMap.remove(pid);
            }

            foreach (pid; failed_pids)
            {
                fileLogger.errorf("Worker PID %s for Chunk %d failed.",
                    pid.processID, this.pcbMap[pid].chunk_id);

                this.memoryUsageMB -= chunk_sizes[this.pcbMap[pid].chunk_id]; // decrease memory usage
                this.pcbMap.remove(pid);
            }

            foreach (pid; retry_pids)
            {
                // Use the same 'extract data first' pattern
                auto old_pcb = this.pcbMap[pid];
                size_t cid = old_pcb.chunk_id;
                int next_attempt = old_pcb.attempt + 1;

                // Remove the OLD PID key from the map immediately
                this.pcbMap.remove(pid);

                if (next_attempt > maxRetries)
                {
                    fileLogger.errorf("Chunk %d failed after max retries. Reclaiming memory.", cid);
                    this.memoryUsageMB -= chunk_sizes[cid];
                    continue;
                }

                // Spawn new process
                Pid new_pid = this.runner.spawn_worker(this.planPath, cid);
                if (new_pid is Pid.init)
                {
                    this.memoryUsageMB -= chunk_sizes[cid];
                    continue;
                }

                // Update the PCB object and re-insert with the NEW PID key
                old_pcb.pid = new_pid;
                old_pcb.attempt = next_attempt;
                old_pcb.start_time = Clock.currTime();

                this.pcbMap[new_pid] = old_pcb;
                fileLogger.warningf("Retrying Chunk %d (Attempt %d). Spawned new Worker PID %s.",
                    cid, next_attempt, new_pid.processID);
            }

            // Phase 4: Tick Wait
            Thread.sleep(dur!"msecs"(tickIntervalMS));
        }
    }
}

public class ProcessRunner
{
    private string pythonPath;
    private string scriptPath;

    this(TaskMode mode, string pythonPath)
    {
        this.pythonPath = pythonPath;
        fileLogger.infof("Python interpreter set to: %s", pythonPath);

        string scriptName;
        final switch (mode) // 'final switch' ensures you handle every TaskMode enum
        {
        case TaskMode.ALIGN:
            scriptName = "align_worker.py";
            break;
        case TaskMode.TEST:
            scriptName = "test_worker.py";
            break;
        case TaskMode.TILING:
            scriptName = "tiling_worker.py";
            break;
        case TaskMode.MOCK:
            scriptName = "mock_worker.py";
            break;
        }

        this.scriptPath = buildPath(thisExePath().dirName(), "..", "engine", scriptName).absolutePath();
        enforce(this.scriptPath.exists,
            "CRITICAL: Engine script missing at " ~ this.scriptPath);

        fileLogger.infof("ProcessRunner initialized for %s mode.", mode);
    }

    public Pid spawn_worker(string jsonPath, size_t chunk_id)
    {
        try
        {
            fileLogger.infof("Spawning process for Chunk %d", chunk_id);
            return spawnProcess([
                pythonPath, scriptPath, jsonPath, chunk_id.to!string
            ]);
        }
        catch (Exception e)
        {
            fileLogger.errorf("System error spawning Chunk %d: %s", chunk_id, e.msg);
            return Pid.init; // Return an invalid Pid
        }
    }
}
