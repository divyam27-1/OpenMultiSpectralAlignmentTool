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
import std.container.dlist;
import std.math : pow;
import std.path;
import std.datetime.systime : SysTime, Clock;
import std.logger;
import std.exception : enforce;

SysTime current_time;
string log_filename;
FileLogger fileLogger;

static this()
{
    if (!exists("log"))
        mkdir("log");
    current_time = Clock.currTime();
    log_filename = "log\\processing_" ~ current_time.toISOExtString().replace(":", "-") ~ ".log";
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

public class ProcessController
{
    private string pythonPath;
    private string scriptPath;

    this(string pyPath, string sPath)
    {
        this.pythonPath = pyPath;
        this.scriptPath = sPath;
    }

    public void finish_chunk(string json_path, size_t chunk_id, int attempt = 0)
    {
        int MAX_ATTEMPTS = 3;
        ProcessResult res = this.run_chunk(json_path, chunk_id);

        if (res.success)
        {
            return;
        }

        if (res.exitCode == 1)
        {
            return;
        }

        // Error code 2 means process failed due to logical error, we need to retry
        if (res.exitCode == 2 && attempt < MAX_ATTEMPTS)
        {
            // Exponential backoff before retrying
            int backoffTime = cast(int) pow(1.42, attempt) * 1000; // in milliseconds
            fileLogger.warningf("Retrying Chunk %d after %d ms (Attempt %d/%d)",
                chunk_id, backoffTime, attempt + 1, MAX_ATTEMPTS);
            Thread.sleep(dur!"msecs"(backoffTime));
            return this.finish_chunk(json_path, chunk_id, attempt + 1);
        }

        fileLogger.errorf("Chunk %d failed after %d attempts. Skipping.", chunk_id, MAX_ATTEMPTS);

        return;
    }

    private Pid spawn_worker(string jsonPath, size_t chunk_id)
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

public class Scheduler
{
    private ProcessController runner;
    private string planPath;

    private DList!size_t taskQueue;
    private ProcessControlBlock[Pid] pcbMap;
    private size_t memoryUsageMB;
    private size_t maxMemoryMB;

    private int maxRetries = 3;
    private int tickIntervalMS = 500;

    this(ProcessController runner, string planPath)
    {
        this.runner = runner;
        this.planPath = planPath;
    }

    public void execute_plan()
    {
        // Read plan from JSON
        auto plan = cast(JSONValue) parseJSON(readText(this.planPath));
        size_t num_chunks = plan.array.length;
        size_t[] chunk_sizes = plan.array.map!(c => c["chunk_size"].to!size_t).array;

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
                if (worker_pid == Pid.init)
                {
                    fileLogger.errorf("Failed to spawn worker for Chunk %d. Skipping", next_chunk_id);
                    continue;
                }

                ProcessControlBlock worker_pcb = ProcessControlBlock();

                worker_pcb.pid = worker_pid;
                worker_pcb.chunk_id = next_chunk_id;
                worker_pcb.json_path = this.planPath;
                worker_pcb.start_time = SysTime.now();
                worker_pcb.attempt = 0;

                this.pcbMap[worker_pid] = worker_pcb;
                this.memoryUsageMB += chunk_sizes[next_chunk_id]; // increase memory usage
            }

            // Phase 2 : Monitor Workers
            Pid[] completed_pids = [];
            Pid[] failed_pids = [];
            Pid[] retry_pids = [];
            foreach (pid, pcb; this.pcbMap)
            {
                auto result = tryWaitProcess(pid);

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
                            pid, pcb.chunk_id, result.status);
                        failed_pids ~= pid;
                        break;
                    }
                }
            }

            // Phase 3: Cleanup and Retry
            foreach (pid; completed_pids)
            {
                fileLogger.infof("Worker PID %s for Chunk %d completed successfully.",
                    pid, this.pcbMap[pid].chunk_id);

                this.memoryUsageMB -= chunk_sizes[this.pcbMap[pid].chunk_id]; // decrease memory usage
                this.pcbMap.remove(pid);
            }

            foreach (pid; failed_pids)
            {
                fileLogger.errorf("Worker PID %s for Chunk %d failed.",
                    pid, this.pcbMap[pid].chunk_id);

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
                if (new_pid == Pid.init)
                {
                    this.memoryUsageMB -= chunk_sizes[cid];
                    continue;
                }

                // Update the PCB object and re-insert with the NEW PID key
                old_pcb.pid = new_pid;
                old_pcb.attempt = next_attempt;
                old_pcb.start_time = Clock.currTime();

                this.pcbMap[new_pid] = old_pcb;
            }

            // Phase 4: Tick Wait
            Thread.sleep(dur!"msecs"(tickIntervalMS));
        }
    }
}

public ProcessController get_runner(TaskMode mode, string pythonPath)
{
    string baseDir = thisExePath().dirName();

    string scriptName;
    switch (mode)
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
    default:
        scriptName = "mock_worker.py";
        break;
    }

    string scriptPath = buildPath(baseDir, "..", "engine", scriptName).absolutePath();

    enforce(scriptPath.exists,
        "CRITICAL ERROR: Python worker script not found at: " ~ scriptPath);

    fileLogger.infof("Runner initialized. Engine script resolved to: %s", scriptPath);

    return new ProcessController(pythonPath, scriptPath);
}
