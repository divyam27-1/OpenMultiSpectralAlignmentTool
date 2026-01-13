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

    private int cpuCheckStreak = 0;
    private const int cpuCheckStreakThreshold = 5;

    this(ProcessRunner runner)
    {
        this.runner = runner;
        this.monitor = new ProcessMonitor(new WindowsMemoryFetcher());
        this.monitor.refresh();

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

        this.updateMonitor();
    }

    public SpawnVerdict trySpawn(size_t chunkId, size_t chunkSize)
    {
        this.updateMonitor();

        SpawnVerdict verdict = this.canSpawn(chunkId, chunkSize);
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

    public SpawnVerdict canSpawn(size_t chunkId, size_t expectedChunkSize)
    {
        import std.math : isNaN;
        import std.format : format;

        // 1. Theoretical Check
        if (currentMemoryUsage + expectedChunkSize > maxMemory)
        {
            managerLogger.infof("[CAN_SPAWN] Chunk %d: FAIL (Theoretical) | Req: %dMB + Pool: %dMB > Max: %dMB",
                chunkId, expectedChunkSize / 1024 / 1024, currentMemoryUsage / 1024 / 1024, maxMemory / 1024 / 1024);
            return SpawnVerdict.LIMIT_REACHED_MEM;
        }

        // 2. RAM Check
        size_t requiredHeadroom = max(memoryHeadroom, memoryHeadroom / 2 + expectedChunkSize);
        if (availableSystemRAM < requiredHeadroom)
        {
            managerLogger.infof("[CAN_SPAWN] Chunk %d: FAIL (Sys RAM) | Avl: %dMB < ReqHeadroom: %dMB",
                chunkId, availableSystemRAM / 1024 / 1024, requiredHeadroom / 1024 / 1024);
            return SpawnVerdict.SYSTEM_BUSY_RAM;
        }

        // 3. CPU Check Logic
        double totalCpuUsage = 0;
        int warmingUp = 0;
        foreach (pid; pcbMap.keys)
        {
            auto report = monitorMap.get(pid, ProcessMonitorReport(0, 0, double.nan));
            totalCpuUsage += isNaN(report.cpu) ? 95.0 : report.cpu;
            if (report.cpu < 25.0)
                warmingUp++;
        }

        double effectiveCoresUsed = totalCpuUsage / 100.0 + warmingUp * 0.95;
        if (effectiveCoresUsed + 1 > effectiveCoreCount)
        {
            managerLogger.infof(
                "[CAN_SPAWN] Chunk %d: FAIL (CPU) | UsedCores: %.1f vs Effective Cores: %.1f (Warm: %d)",
                chunkId, effectiveCoresUsed, effectiveCoreCount, warmingUp);
            return SpawnVerdict.SYSTEM_BUSY_CPU;
        }

        // 3.5 If CPU check has passed, wait for it to stabilize
        if (cpuCheckStreak++ < cpuCheckStreakThreshold)
        {
            managerLogger.infof(
                "[CAN_SPAWN] Chunk %d: STABILIZING (CPU) | UsedCores: %.1f vs Effective Cores: %.1f (Warm: %d)",
                chunkId, effectiveCoresUsed, effectiveCoreCount, warmingUp);
            return SpawnVerdict.SYSTEM_BUSY_CPU;
        }

        // 4. Success Log
        managerLogger.infof("[CAN_SPAWN] Chunk %d: OK | Pool: %dMB | AvlRAM: %dMB | Cores: %.1f/%.1f",
            chunkId, currentMemoryUsage / 1024 / 1024, availableSystemRAM / 1024 / 1024,
            effectiveCoresUsed, effectiveCoreCount);

        cpuCheckStreak = 0;
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

    public size_t[2] getRAMDetails()
    {
        return [this.currentMemoryUsage, this.availableSystemRAM];
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

    string[string] workerEnv;

    this(string pythonPath, string scriptName, TaskMode mode, string planPath)
    {
        this.pythonPath = pythonPath;
        this.mode = mode;
        this.planPath = planPath;
        
        this.workerEnv = environment.toAA(); 
        this.workerEnv["OMP_NUM_THREADS"] = "1";
        this.workerEnv["MKL_NUM_THREADS"] = "1";
        this.workerEnv["OPENBLAS_NUM_THREADS"] = "1";
        this.workerEnv["VECLIB_MAXIMUM_THREADS"] = "1";
        this.workerEnv["OPENCV_FOR_THREADS_NUM"] = "1";

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
            ], this.workerEnv);
        }
        catch (Exception e)
        {
            managerLogger.errorf("System error spawning Chunk %d: %s", chunk_id, e.msg);
            return Pid.init;
        }
    }
}
