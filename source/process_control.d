module process_control;

import core.thread;
import core.time;
import core.atomic;
import core.stdc.signal;

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

import process_control_h;
import process_monitor;
import process_monitor_h : WindowsMemoryFetcher, ProcessMonitorReport;
import appstate;
import config;
import tui_h : ProgressBar;
import loggers : processLogger;

shared bool shutdownRequested = false;

public class Scheduler
{
    private ProcessRunner runner;
    private string planPath;

    private ProcessMonitor monitor;

    private DList!size_t taskQueue;
    private ProcessControlBlock[Pid] pcbMap;
    private ProcessMonitorReport[Pid] monitorMap;

    // initialized to default values just in case something goes wrong with reading cfg
    private size_t memoryUsage = 0;
    private size_t maxMemory = 2048 * 1024 * 1024;
    private int maxRetries = 3;
    private int tickIntervalMS = 75;
    private ulong memoryHeadroom = 256 * 1024 * 1024;
    private ulong availableRAM = 0;

    // generats TUI and progress bar
    private ProgressBar progBar;
    private int completed = 0, running = 0, failed = 0;

    private int i = 0;

    this(ProcessRunner runner, string planPath, size_t maxMemoryMB = 2048)
    {
        this.runner = runner;
        this.monitor = new ProcessMonitor(new WindowsMemoryFetcher());
        this.planPath = planPath;
        this.maxMemory = maxMemoryMB * 1024 * 1024;

        auto cfg = loadConfig(buildPath(thisExePath().dirName, "omspec.cfg").absolutePath());
        this.maxRetries = cfg.max_retries;
        this.tickIntervalMS = cfg.tick_interval_ms;
        this.maxMemory = cfg.max_memory_mb * 1024 * 1024;
    }

    public bool execute_plan()
    {
        // Read plan from JSON
        auto plan = parseJSON(readText(this.planPath));
        size_t num_chunks = plan.array.length;
        if (num_chunks <= 0)
        {
            processLogger.errorf("Plan is empty");
            return false;
        }

        size_t[] chunk_sizes = plan.array.map!(c => cast(size_t) c["chunk_size"].get!long).array;

        foreach (i; 0 .. num_chunks)
        {
            this.taskQueue.insertBack(i);
        }

        // Spawn progress bar
        progBar = ProgressBar(cast(int) num_chunks);

        while (!this.taskQueue.empty || this.pcbMap.length > 0)
        {
            if (atomicLoad(shutdownRequested))
            {
                writeln(
                    "Shutdown requested by user. Stopping scheduler and Terminating all workers..."
                );
                processLogger.warning("Shutdown requested by user. Stopping scheduler...");
                break; // Exit the loop to trigger the scope(exit) cleanup
            }

            // Phase 1 : Worker Spawning
            while (!this.taskQueue.empty && this.memoryUsage < this.maxMemory) // TODO: make this somehow relate to the actual memory on RAM
            {
                size_t next_chunk_id = this.taskQueue.front;
                this.taskQueue.removeFront();

                Pid worker_pid = this.runner.spawn_worker(this.planPath, next_chunk_id);
                if (worker_pid is Pid.init)
                {
                    processLogger.errorf("Failed to spawn worker for Chunk %d. Skipping", next_chunk_id);
                    continue;
                }

                ProcessControlBlock worker_pcb = ProcessControlBlock();

                worker_pcb.pid = worker_pid;
                worker_pcb.chunk_id = next_chunk_id;
                worker_pcb.json_path = this.planPath;
                worker_pcb.start_time = Clock.currTime();
                worker_pcb.attempt = 0;

                this.pcbMap[worker_pid] = worker_pcb;
                this.memoryUsage += chunk_sizes[next_chunk_id]; // increase memory usage

                processLogger.infof("Spawned Worker PID %s for Chunk %d. Current Memory Usage: %d MB",
                    worker_pid.processID, next_chunk_id, this.memoryUsage);
            }

            // Phase 2 : Monitor Workers
            Pid[] completed_pids = [];
            Pid[] failed_pids = [];
            Pid[] retry_pids = [];
            Pid[] running_pids = [];
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
                        processLogger.errorf("Worker PID %s for Chunk %d exited with unknown code %d",
                            pid.processID, pcb.chunk_id, result.status);
                        failed_pids ~= pid;
                        break;
                    }
                    continue;
                }

                running_pids ~= pid;
            }

            // Phase 3: Cleanup and Retry
            this.monitor.updateTracking(cast(uint[])running_pids.map!(p => p.processID).array);
            this.monitor.refresh();
            this.monitorMap.clear();
            this.availableRAM = this.monitor.getAvailableSystemRAM();
            foreach (Pid pid; running_pids)
                this.monitorMap[pid] = this.monitor.getUsage(pid.processID);

            foreach (pid; completed_pids)
            {
                processLogger.infof("Worker PID %s for Chunk %d completed successfully.",
                    pid.processID, this.pcbMap[pid].chunk_id);

                this.memoryUsage -= chunk_sizes[this.pcbMap[pid].chunk_id]; // decrease memory usage
                this.pcbMap.remove(pid);
                this.monitorMap.remove(pid);

                completed++;
            }

            foreach (pid; failed_pids)
            {
                processLogger.errorf("Worker PID %s for Chunk %d failed.",
                    pid.processID, this.pcbMap[pid].chunk_id);

                this.memoryUsage -= chunk_sizes[this.pcbMap[pid].chunk_id]; // decrease memory usage
                this.pcbMap.remove(pid);
                this.monitorMap.remove(pid);

                failed++;
            }

            foreach (pid; retry_pids)
            {
                // Use the same 'extract data first' pattern
                auto old_pcb = this.pcbMap[pid];
                size_t cid = old_pcb.chunk_id;
                int next_attempt = old_pcb.attempt + 1;

                // Remove the OLD PID key from the map immediately
                this.pcbMap.remove(pid);
                this.monitorMap.remove(pid);

                if (next_attempt > maxRetries)
                {
                    processLogger.errorf("Chunk %d failed after max retries. Reclaiming memory.", cid);
                    this.memoryUsage -= chunk_sizes[cid];

                    failed++;
                    continue;
                }

                // Spawn new process
                Pid new_pid = this.runner.spawn_worker(this.planPath, cid);
                if (new_pid is Pid.init)
                {
                    this.memoryUsage -= chunk_sizes[cid];
                    taskQueue.insertBack(cid);
                    continue;
                }

                // Update the PCB object and re-insert with the NEW PID key
                old_pcb.pid = new_pid;
                old_pcb.attempt = next_attempt;
                old_pcb.start_time = Clock.currTime();

                this.pcbMap[new_pid] = old_pcb;
                processLogger.warningf("Retrying Chunk %d (Attempt %d). Spawned new Worker PID %s.",
                    cid, next_attempt, new_pid.processID);
            }

            // Phase 4: Tick Wait
            progBar.update(completed, cast(int) this.pcbMap.length, failed);
            if (this.i++ % 70 == 0)
                this.logResourceUsage(running_pids.map!(p => monitorMap[p]).array);
            Thread.sleep(dur!"msecs"(tickIntervalMS));
        }

        progBar.finish();
        return failed == 0 ? true : false;

        // On exit, terminate all workers
        scope (exit)
        {
            if (pcbMap.length > 0)
            {
                processLogger.info("Emergency cleanup: Terminating all active workers...");
                foreach (pid, pcb; pcbMap)
                {
                    try
                    {
                        kill(pid);
                    }
                    catch (Exception e)
                    {
                        processLogger.errorf("Error terminating Worker PID %s: %s", pid.processID, e
                                .msg);
                    }
                }
            }

            progBar.finish();
            this.monitor.shutDownWorker();
        }
    }

    public string get_summary()
    {
        import std.format;

        string line1 = format("Final Status: %d Success, %d Failed %d Total",
            this.completed, this.failed, this.completed + this.failed);

        if (this.failed > 0)
        {
            line1 ~= "\nCheck logs/process_ and logs/worker_ for specific error details on failed chunks.";
        }

        return line1;
    }

    public void logResourceUsage(ProcessMonitorReport[] to_log)
    {
        foreach (ProcessMonitorReport report; to_log)
        {
            processLogger.infof("Usage of PID %d: CPU(%.2f), RAM(%d | %d)",
                report.pid, report.cpu, report.memory / (1024 * 1024), this.availableRAM / (1024 * 1024));
        }
    }
}

public class ProcessRunner
{
    private string pythonPath;
    private string scriptPath;
    private TaskMode mode;
    private string workflow;

    this(string pythonPath, string scriptName, TaskMode mode)
    {
        this.pythonPath = pythonPath;
        this.mode = mode;

        // Resolve the workflow string once at instantiation
        this.workflow = () {
            final switch (mode)
            {
            case TaskMode.ALIGN:
                return "ALIGN";
            case TaskMode.TEST:
                return "TEST";
            case TaskMode.TILING:
                return "TILING";
            case TaskMode.MOCK:
                return "MOCK";
            }
        }();

        processLogger.infof("Python interpreter set to: %s", pythonPath);

        this.scriptPath = buildPath(thisExePath().dirName(), "..", "engine", scriptName).absolutePath();
        enforce(this.scriptPath.exists,
            "CRITICAL: Engine script missing at " ~ this.scriptPath);

        processLogger.infof("ProcessRunner initialized for %s mode.", this.workflow);
    }

    public Pid spawn_worker(string jsonPath, size_t chunk_id)
    {
        try
        {
            processLogger.infof("[%s] Spawning process for Chunk %d", this.workflow, chunk_id);

            return spawnProcess([
                pythonPath,
                scriptPath,
                this.workflow,
                jsonPath,
                chunk_id.to!string
            ]);
        }
        catch (Exception e)
        {
            processLogger.errorf("System error spawning Chunk %d: %s", chunk_id, e.msg);
            return Pid.init;
        }
    }
}

extern (C) void handleInterrupt(int sig) nothrow @nogc @system
{
    import core.atomic : atomicStore;

    atomicStore(shutdownRequested, true);
}
