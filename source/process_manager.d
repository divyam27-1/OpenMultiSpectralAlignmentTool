module process_manager;

import std.stdio;
import std.parallelism : totalCPUs;
import std.process : tryWait;
import std.process;
import std.algorithm : map;
import std.algorithm.comparison : max;
import std.datetime : Clock;
import std.datetime.stopwatch;
import std.path : buildPath, absolutePath;
import std.file : thisExePath, exists;
import std.path : dirName;
import std.exception : enforce;
import std.conv;
import std.array;
import std.container.dlist;
import core.thread.fiber;

import process_manager_h;
import process_monitor_h;
import process_herald_h;
import process_monitor;
import process_herald;
import config;
import appstate : mode;
import loggers : managerLogger;

class ProcessManager
{
    bool busy = false;
    private Fiber managerFiber;

    private ProcessRunner runner;
    private ProcessMonitor monitor;
    private ProcessHerald herald;

    private ProcessControlBlock[uint] pcbMap;
    private ProcessMonitorReport[uint] monitorMap;
    private ChunkControlBlock currentChunk;

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
        this.herald = new ProcessHerald();

        this.monitor.refresh();
        this.currentChunk = ChunkControlBlock();

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

    void putChunk(uint chunkId, uint numImages)
    {
        this.busy = true;

        managerFiber = new Fiber({
            this.processChunk(chunkId, numImages);
            this.busy = false;
        },
            64 * 1024); // 64 kB stack
    }

    // TODO: add functionality to kill or throttle idle workers if cpu is being over utilized
    private void processChunk(uint chunkId, uint numImages)
    {
        this.currentChunk = ChunkControlBlock(chunkId, numImages, chunkSize); // I made a constructor for the struct that self populates the Dynamic Array 
        managerLogger.infof("Starting Processing: Chunk %d with %d images", chunkId, numImages);

        // Execution Loop
        while (!currentChunk.isComplete())
        {
            HeraldMessage[] messages = herald.popMessages();
            foreach (msg; messages)
                handleWorkerResponse(msg);

            reapWorkers();
            dispatchTasksToIdleWorkers();

            if (currentChunk.hasUnassignedWork() && !hasIdleWorkers() && canSpawn())
                spawnNewWorker();

            Fiber.yield();
        }

        managerLogger.infof("Chunk %d processing cycle finished.", chunkId);
    }

    private void dispatchTasksToIdleWorkers()
    {
        foreach (workerId, ref pcb; pcbMap)
        {
            if (pcb.state != WorkerState.IDLE)
                continue;

            uint imgIdx = currentChunk.findNextPendingImage();
            if (imgIdx == uint.max)
                break; // No more work to give for now

            // Update PCB
            pcb.state = WorkerState.BUSY;
            pcb.activeImageIdx = imgIdx;
            pcb.taskTimer.reset();

            // Update CCB
            currentChunk.taskStates[imgIdx] = TaskState.INPROGRESS;

            // Send command to Herald
            herald.sendTask(workerId, currentChunk.chunkId, imgIdx);

            managerLogger.infof("Assigned: Worker %d -> Chunk %d, Image %d",
                workerId, currentChunk.chunkId, imgIdx);
        }
    }

    private void handleWorkerResponse(HeraldMessage msg)
    {
        ProcessControlBlock* pcb = msg.workerId in pcbMap;
        if (!pcb)
        {
            managerLogger.errorf("Received message for unknown PID %d: %s", msg.workerId, msg
                    .msgType);
            return;
        }

        switch (msg.msgType)
        {
        case WorkerMessages.Heartbeat:
            if (pcb.state == WorkerState.SPAWNING)
            {
                pcb.state = WorkerState.IDLE;
                pcb.taskTimer.reset(); // Stop the spawning watchdog timer
                managerLogger.infof("Worker %d handshake successful. Ready for tasks.", msg
                        .workerId);
            }
            break;

        case WorkerMessages.TaskFinish:
            const uint imgIdx = pcb.activeImageIdx;
            if (imgIdx == uint.max)
            {
                managerLogger.errorf("Worker %d finished, but activeImageIdx was null.", msg
                        .workerId);
                pcb.state = WorkerState.IDLE;
                return;
            }

            auto status = cast(TaskFinishCodes) msg.payload[0];
            processTaskFinish(pcb, imgIdx, status);

            pcb.state = WorkerState.IDLE;
            pcb.activeImageIdx = uint.max;
            pcb.taskTimer.stop();
            break;

        case WorkerMessages.WorkerError:
        case WorkerMessages.MessageInvalid:
            managerLogger.errorf("Worker %d critical failure: %s", msg.workerId, msg.msgType);
            pcb.state = WorkerState.UNRESPONSIVE;
            break;

        case WorkerMessages.WorkerStop:
            pcb.state = WorkerState.KILLED;
            break;

        default:
            break;
        }
    }

    private void processTaskFinish(ProcessControlBlock* pcb, uint imgIdx, TaskFinishCodes status)
    {
        switch (status)
        {
        case TaskFinishCodes.Success:
            currentChunk.taskStates[imgIdx] = TaskState.COMPLETED;
            break;

        case TaskFinishCodes.RetryRequested:
            if (currentChunk.retryCount[imgIdx] < 3)
            {
                currentChunk.retryCount[imgIdx]++;
                currentChunk.taskStates[imgIdx] = TaskState.PENDING;
                managerLogger.warnf("Task %d:%d retry %d/3", currentChunk.chunkId, imgIdx, currentChunk
                        .retryCount[imgIdx]);
            }
            else
            {
                currentChunk.taskStates[imgIdx] = TaskState.FAILED;
                managerLogger.errorf("Task %d:%d failed after max retries", currentChunk.chunkId, imgIdx);
            }
            break;

        default:
            currentChunk.taskStates[imgIdx] = TaskState.FAILED;
            break;
        }
    }

    private void reapWorkers()
    {
        auto activePids = pcbMap.keys;

        foreach (pid; activePids)
        {
            auto pcb = &pcbMap[pid];
            bool needsCleanup = false;

            auto result = tryWait(pcb.pid);
            if (result.terminated)
            {
                managerLogger.criticalf("Worker %d exited unexpectedly (OS Status: %d)", pid, result
                        .status);
                needsCleanup = true;
            }
            else if (pcb.state == WorkerState.SPAWNING && pcb.taskTimer.peek().total!"seconds" > 30)
            {
                managerLogger.errorf("Worker %d timed out during spawn handshake. Killing.", pid);
                kill(pcb.pid);
                needsCleanup = true;
            }
            else if (pcb.state == WorkerState.UNRESPONSIVE || pcb.state == WorkerState.KILLED)
            {
                kill(pcb.pid);
                needsCleanup = true;
            }

            if (needsCleanup)
                cleanupWorker(pid);
        }
    }

    private void cleanupWorker(uint pid)
    {
        if (auto pPcb = pid in pcbMap)
        {
            if (pPcb.activeImageIdx != uint.max)
            {
                currentChunk.taskStates[pPcb.activeImageIdx] = TaskState.PENDING;
                managerLogger.infof("Rescued Image %d from dead Worker %d", pPcb.activeImageIdx, pid);
            }

            herald.deregisterWorker(pid);
            pcbMap.remove(pid);
            monitorMap.remove(pid);
        }
    }

    private bool hasIdleWorkers()
    {
        foreach (pid, ref pcb; pcbMap)
            if (pcb.state == WorkerState.IDLE)
                return true;

        return false;
    }

    public void spawnNewWorker()
    {
        ZMQEndpoints endpoints = herald.reserveEndpoints();

        try
        {
            Pid pid = this.runner.spawn_worker(
                currentChunk.chunkId,
                endpoints
            );

            if (pid is Pid.init)
            {
                managerLogger.errorf("OS failed to spawn worker for Chunk %d", currentChunk.chunkId);
                herald.unreserveEndpoints(endpoints);
                return;
            }

            uint workerPid = cast(uint) pid.processID;
            bool registered = herald.registerWorker(workerPid, endpoints);

            if (!registered)
            {
                managerLogger.errorf("Herald failed to register Worker %d", workerPid);
                kill(pid);
                return;
            }

            ProcessControlBlock pcb;
            pcb.pid = pid;
            pcb.state = WorkerState.SPAWNING;
            pcb.activeChunkId = currentChunk.chunkId;
            pcb.activeImageIdx = uint.max;
            pcb.taskTimer = StopWatch(AutoStart.yes);
            pcb.lastMemoryUsage = 200 * 1024 * 1024; // 200 MB which is approx amount of barebones python runtime

            this.pcbMap[workerPid] = pcb;

            managerLogger.infof("Worker %d spawned. ZMQ: IN=%s OUT=%s",
                workerPid, endpoints.inEndpoint, endpoints.outEndpoint);
        }
        catch (Exception e)
        {
            managerLogger.errorf("Critical failure spawning worker: %s", e.msg);
            herald.unreserveEndpoints(endpoints);
        }

        this.updateMonitor();
        this.monitor.updateTracking(pcbMap.keys.array);
    }

    public SpawnVerdict canSpawn(size_t chunkId)
    {
        import std.math : isNaN;

        // 1. Hard Core Limit
        if (this.getActiveProcessCount() + 1 > this.effectiveCoreCount)
        {
            managerLogger.infof("[CAN_SPAWN] Chunk %d: FAIL (Core Limit) | Count: %d >= Cores: %.1f",
                chunkId, this.getActiveProcessCount(), effectiveCoreCount);
            return SpawnVerdict.SYSTEM_BUSY_CPU;
        }

        // 2. RAM Headroom Check
        if (availableSystemRAM < memoryHeadroom)
        {
            managerLogger.infof("[CAN_SPAWN] Chunk %d: FAIL (RAM Headroom) | Avl: %dMB < Headroom: %dMB",
                chunkId, availableSystemRAM / 1024 / 1024, memoryHeadroom / 1024 / 1024);
            return SpawnVerdict.SYSTEM_BUSY_RAM;
        }

        // 3. CPU Load Logic
        double totalCpuUsage = 0;
        int warmingUp = 0;

        foreach (pid; pcbMap.keys)
        {
            auto report = monitorMap.get(pid, ProcessMonitorReport(0, 0, double.nan));
            totalCpuUsage += isNaN(report.cpu) ? 95.0 : report.cpu;
            if (report.cpu < 10.0)
                warmingUp++;
        }

        double effectiveCoresUsed = (totalCpuUsage / 100.0) + (warmingUp * 0.5);
        if (effectiveCoresUsed + 1.0 > effectiveCoreCount)
        {
            managerLogger.infof("[CAN_SPAWN] Chunk %d: FAIL (CPU Load) | EffectiveUsed: %.2f / %.1f",
                chunkId, effectiveCoresUsed, effectiveCoreCount);
            return SpawnVerdict.SYSTEM_BUSY_CPU;
        }

        // 4. Stabilization Check
        if (cpuCheckStreak++ < cpuCheckStreakThreshold)
        {
            managerLogger.infof(
                "[CAN_SPAWN] Chunk %d: STABILIZING (CPU) | UsedCores: %.1f vs Effective Cores: %.1f (Warm: %d)",
                chunkId, effectiveCoresUsed, effectiveCoreCount, warmingUp);
            return SpawnVerdict.SYSTEM_BUSY_CPU;
        }

        managerLogger.infof("[CAN_SPAWN] Chunk %d: OK | AvlRAM: %dMB | Cores: %.1f/%.1f",
            chunkId, availableSystemRAM / 1024 / 1024, effectiveCoresUsed, effectiveCoreCount);

        cpuCheckStreak = 0;
        return SpawnVerdict.OK;
    }

    private void updateMonitor()
    {
        this.monitor.refresh();
        this.monitorMap.clear();

        size_t total = 0;
        foreach (pid; this.pcbMap.keys)
        {
            ProcessMonitorReport report = this.monitor.getUsage(cast(uint) pid);
            this.monitorMap[pid] = report;

            // If RAM is 0, it means the monitor hasn't caught the process yet
            total += report.memory > 0 ? report.memory : this.pcbMap[pid].lastMemoryUsage;
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
        foreach (pid; pcbMap.keys)
        {
            try
            {
                kill(pid);
                managerLogger.infof("Forced to kill process %d", pid);
            }
            catch (Exception e)
                managerLogger.errorf("Failed to kill process %d: %s", pid, e.msg);
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
