module process_manager;

import std.stdio;
import std.parallelism : totalCPUs;
import std.process : tryWait;
import std.process;
import std.algorithm : map;
import std.algorithm.comparison : max;
import std.datetime : Clock;
import std.path : buildPath, absolutePath;
import std.file : thisExePath, exists;
import std.path : dirName;
import std.exception : enforce;
import std.conv;
import std.array;

import process_manager_h;
import process_monitor_h;
import process_monitor;
import config;
import loggers : managerLogger;

class ProcessManager
{
    private ProcessRunner runner;
    private ProcessMonitor monitor;

    private ProcessControlBlock[Pid] pcbMap;
    private ProcessMonitorReport[Pid] monitorMap;

    private immutable size_t maxMemory;
    private immutable size_t physicalCoreCount;
    private immutable size_t logicalCoreCount;
    private immutable double effectiveCoreCount;
    private immutable int smtRatio;
    private immutable size_t memoryHeadroom = 256 * 1024 * 1024;
    private immutable int maxRetries;

    private size_t currentMemoryUsage;
    private size_t availableSystemRAM;

    this(ProcessRunner runner)
    {
        this.runner = runner;
        this.monitor = new ProcessMonitor(new WindowsMemoryFetcher());

        auto cfg = loadConfig(buildPath(thisExePath().dirName, "omspec.cfg").absolutePath());
        this.maxMemory = cfg.max_memory_mb * 1024 * 1024;
        this.smtRatio = cfg.smt_ratio;
        this.maxRetries = cfg.max_retries;

        uint systemCoreCount = getPhysicalCoreCount();
        this.logicalCoreCount = totalCPUs;
        this.physicalCoreCount = systemCoreCount == 1 ? this.logicalCoreCount / this.smtRatio
            : systemCoreCount;
        this.effectiveCoreCount = this.physicalCoreCount + 0.2 * (
            this.logicalCoreCount - this.physicalCoreCount);
    }

    public SpawnVerdict trySpawn(size_t chunkId, size_t chunkSize)
    {
        this.updateMonitor();

        SpawnVerdict verdict = this.canSpawn(chunkSize);
        if (verdict != SpawnVerdict.OK)
            return verdict;

        Pid pid = this.runner.spawn_worker(chunkId);
        if (pid is Pid.init)
        {
            managerLogger.errorf("OS failed to spawn worker for Chunk %d", chunkId);
            return SpawnVerdict.SPAWN_FAILURE;
        }

        ProcessControlBlock pcb;
        pcb.pid = pid;
        pcb.chunk_id = cast(uint) chunkId;
        pcb.start_time = Clock.currTime();
        pcb.attempt = 1;
        pcb.estimated_memory = chunkSize;

        this.pcbMap[pid] = pcb;

        this.currentMemoryUsage += pcb.estimated_memory;

        managerLogger.infof("Spawned Worker PID %d (Chunk %d). Memory: %d MB",
            pid.processID, chunkId, pcb.estimated_memory / (1024 * 1024));

        return SpawnVerdict.OK;
    }

    public SpawnVerdict canSpawn(size_t expectedChunkSize)
    {
        import std.math : isNaN;

        // 1. Theoretical Check
        if (currentMemoryUsage + expectedChunkSize > maxMemory)
            return SpawnVerdict.LIMIT_REACHED_MEM;

        // 2. RAM Check
        if (availableSystemRAM < max(memoryHeadroom, memoryHeadroom / 2 + expectedChunkSize))
            return SpawnVerdict.SYSTEM_BUSY_RAM;

        // 3. CPU Check
        double totalCpuUsage = 0;
        int warmingUp = 0;
        foreach (pid; pcbMap.keys)
        {
            auto report = monitorMap.get(pid, ProcessMonitorReport(0, 0, double.nan));

            // If it's NaN (new process), assume 80% load to prevent over-spawning
            totalCpuUsage += isNaN(report.cpu) ? 80.0 : report.cpu;

            // If a worker has less than 5% usage, it is probably warming up
            if (report.cpu < 5.0)
                warmingUp++;
        }

        double effectiveCoresUsed = totalCpuUsage / 100.0 + warmingUp * 0.6;

        // We can schedule if current use is at least 1 "core" below threshold
        if (effectiveCoresUsed > (effectiveCoreCount - 1.0))
            return SpawnVerdict.SYSTEM_BUSY_CPU;

        return SpawnVerdict.OK;
    }

    public ChunkGraveyard reap()
    {
        ChunkGraveyard graveyard;

        this.updateMonitor();

        foreach (Pid pid; this.pcbMap.keys.dup)
        {
            auto pcb = this.pcbMap[pid];
            auto status = tryWait(pid);
            if (status.terminated)
            {
                // For reaping we take away the real memory usage, for spawning we take an estimate
                // currentMemoryUsage will be synced anyways with the real sum of memory usages in the updateMonitor() function
                // Fallback logic to prevent crash on missing telemetry
                size_t lastKnownMem = this.monitorMap.get(pid, ProcessMonitorReport.init).memory;
                this.currentMemoryUsage -= (lastKnownMem > 0) ? lastKnownMem : pcb.estimated_memory;

                if (status.status == 0)
                    graveyard.completed ~= cast(uint) pcb.chunk_id;
                else if (status.status == 2)
                {
                    if (pcb.attempt++ > this.maxRetries)
                        graveyard.failed ~= cast(uint) pcb.chunk_id;
                    else
                        graveyard.retries ~= cast(uint) pcb.chunk_id;
                }
                else
                    graveyard.failed ~= cast(uint) pcb.chunk_id;

                this.pcbMap.remove(pid);
                this.monitorMap.remove(pid);
            }
        }

        this.monitor.updateTracking(this.pcbMap.keys.map!(p => cast(uint) p.processID).array);
        return graveyard;
    }

    private void updateMonitor()
    {
        this.monitor.refresh();

        size_t total = 0;
        foreach (pid; this.pcbMap.keys)
        {
            ProcessMonitorReport report = this.monitor.getUsage(cast(uint) pid.processID);
            this.monitorMap[pid] = report;

            // If RAM is 0, it means the monitor hasn't caught the process yet
            total += report.memory > 0 ? report.memory : this.pcbMap[pid].estimated_memory;
        }

        this.availableSystemRAM = this.monitor.getAvailableSystemRAM();
        this.currentMemoryUsage = total;
    }

    public size_t getActiveProcessCount() @safe const
    {
        return this.pcbMap.length;
    }

    public void terminateAll()
    {
        foreach (Pid pid; pcbMap.keys)
        {
            try
            {
                kill(pid);
                managerLogger.infof("Forced to kill process %d", pid.processID);
            }
            catch (Exception e)
                managerLogger.errorf("Failed to kill process %d: %s", pid.processID, e.msg);
        }

        this.pcbMap.clear();
        this.monitorMap.clear();
        this.currentMemoryUsage = 0;

        this.monitor.updateTracking([]);
        this.monitor.shutDownWorker();
    }
}

public class ProcessRunner
{
    import appstate;

    private string pythonPath;
    private string scriptPath;
    private TaskMode mode;
    private string workflow;
    private string planPath;

    this(string pythonPath, string scriptName, TaskMode mode, string planPath)
    {
        this.pythonPath = pythonPath;
        this.mode = mode;
        this.planPath = planPath;

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

        managerLogger.infof("Python interpreter set to: %s", pythonPath);

        this.scriptPath = buildPath(thisExePath().dirName(), "..", "engine", scriptName)
            .absolutePath();
        enforce(this.scriptPath.exists,
            "CRITICAL: Engine script missing at " ~ this.scriptPath);

        managerLogger.infof("ProcessRunner initialized for %s mode.", this.workflow);
    }

    public Pid spawn_worker(size_t chunk_id)
    {
        try
        {
            managerLogger.infof("[%s] Spawning process for Chunk %d", this.workflow, chunk_id);

            return spawnProcess([
                pythonPath,
                scriptPath,
                this.workflow,
                this.planPath,
                chunk_id.to!string
            ]);
        }
        catch (Exception e)
        {
            managerLogger.errorf("System error spawning Chunk %d: %s", chunk_id, e.msg);
            return Pid.init;
        }
    }
}
