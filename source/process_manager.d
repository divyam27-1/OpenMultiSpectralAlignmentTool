module process_manager;

import std.stdio;
import std.parallelism : totalCPUs;
import std.concurrency;
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
import std.variant;
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

    // Dependencies
    private ProcessRunner runner;
    private ProcessMonitor monitor;
    private ProcessHerald herald;

    // Worker Maps
    private ProcessControlBlock[uint] pcbMap; // map of all workers
    private ProcessMonitorReport[uint] monitorMap; // map of all workers tracked by monitor
    private WorkerCreationStatus[uint] spawningMap; // map of all workers who are currently SPAWNING
    private ChunkControlBlock currentChunk;

    // Mailboxes
    private DList!Variant monitorMailbox;
    private DList!Variant heraldMailbox;
    private DList!HeraldMessage responseMailbox;

    private immutable size_t maxMemory;
    private immutable size_t physicalCoreCount;
    private immutable size_t logicalCoreCount;
    private immutable double effectiveCoreCount;
    private immutable int smtRatio;
    private immutable size_t memoryHeadroom = 256 * 1024 * 1024;
    private immutable int maxRetries;
    private immutable uint flushLimit = 8; // if a worker has more than 8 completed images in store flush will be triggered

    private size_t currentMemoryUsage;
    private size_t availableSystemRAM;

    private int cpuCheckStreak = 0;
    private const int cpuCheckStreakThreshold = 3;

    private uint progCompleted = 0, progFailed = 0;

    this(ProcessRunner runner)
    {
        this.runner = runner;
        this.monitor = new ProcessMonitor(new WindowsMemoryFetcher());
        this.herald = new ProcessHerald();

        this.monitor.refresh(this.monitorMailbox);
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

    bool putChunk(uint chunkId, uint numImages)
    {
        if (this.busy)
            return false;

        this.busy = true;

        managerFiber = new Fiber({
            try
            {
                this.processChunk(chunkId, numImages);
            }
            catch (Exception e)
            {
                managerLogger.errorf("Critical Manager Error: %s", e.msg);
                this.progFailed += numImages;
            }
            finally
            {
                this.busy = false;
            }
        }, 64 * 1024);

        return true;
    }

    void processTick()
    {
        collectMessages();

        if (this.managerFiber && this.managerFiber.state != Fiber.State.TERM)
            managerFiber.call();
    }

    // TODO later: add functionality to kill or throttle idle workers if cpu is being over utilized
    private void processChunk(uint chunkId, uint numImages)
    {
        this.currentChunk = ChunkControlBlock(chunkId, numImages); // I made a constructor for the struct that self populates the Dynamic Array 
        managerLogger.infof("Starting Processing: Chunk %d with %d images", chunkId, numImages);

        // Execution Loop
        while (!currentChunk.isComplete())
        {
            // Response : Something returned from Worker via ZMQ
            // Message : Something returned from thread via std.concurrency

            // Sensing
            updateMonitor();
            updateSpawningWorkers();

            // Maintenance
            reapWorkers();

            // Task Delegation
            handleAllWorkerResponses();
            dispatchTasksToIdleWorkers();

            // Scaling
            if (currentChunk.hasUnassignedWork() && !hasIdleWorkers() && canSpawn(chunkId) == SpawnVerdict
                .OK)
                initiateNewWorker();

            Fiber.yield();
        }

        managerLogger.infof("Chunk %d processing cycle finished.", chunkId);
    }

    private void collectMessages()
    {
        bool messagesPending = true;

        while (messagesPending)
        {
            bool found = receiveTimeout(0.msecs, // Monitor Messages
                (M_TelemetryUpdate msg) {
                monitorMailbox.insertBack(Variant(msg));
            }, // Herald Messages
                (immutable ZMQEndpoints msg) {
                heraldMailbox.insertBack(Variant(msg));
            },
                (M_RegisterResponse msg) {
                heraldMailbox.insertBack(Variant(msg));
            },
                (M_DeregisterRequest msg) {
                heraldMailbox.insertBack(Variant(msg));
            }, // Unknown Messages
                (Variant any) {
                managerLogger.warning("Manager received unhandled message type: ", any.type);
            }
            );

            if (!found)
            {
                messagesPending = false;
            }
        }
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

    private void handleAllWorkerResponses()
    {
        HeraldMessage[] newResponses = herald.popResponses();
        foreach (res; newResponses)
            responseMailbox.insertBack(res);

        size_t responsesToProcess = 0;
        foreach (m; responseMailbox)
            responsesToProcess++;

        while (responsesToProcess > 0)
        {
            HeraldMessage msg = responseMailbox.front;
            responseMailbox.removeFront();

            bool handled = handleWorkerResponse(msg);
            if (!handled)
                responseMailbox.insertBack(msg);

            responsesToProcess--;
        }
    }

    // Returns True if task is fully handled by this function
    // Returns False if task needs more operations or state changes
    private bool handleWorkerResponse(HeraldMessage msg)
    {
        uint workerId = cast(uint) msg.workerId;
        ProcessControlBlock* pcb = workerId in pcbMap;

        // 1. Ghost Message Check
        if (!pcb)
        {
            managerLogger.errorf("Received message for unknown PID %d: %s (Discarding)", workerId, msg
                    .msgType);
            return true; // Discard: no PCB means we can't do anything anyway
        }

        switch (msg.msgType)
        {
        case WorkerMessages.Heartbeat:
            if (auto pStatus = workerId in spawningMap)
            {
                // Only promote if the Registration Handshake is COMPLETE
                if (*pStatus != WorkerCreationStatus.Created)
                    return false;

                pcb.state = WorkerState.IDLE;
                pcb.taskTimer.reset();
                this.spawningMap.remove(workerId);
                managerLogger.infof("Worker %d handshake successful. Ready for tasks.", workerId);
                return true;
            }

            // If it's already IDLE or BUSY, a heartbeat is just a "keep-alive"
            // Althought non-first heartbeats should be filtered by WorkerConnection.recvMessages() anyways
            return true;

        case WorkerMessages.TaskFinish:
            // If for some reason the task finishes before we've cleared the spawningMap
            // we should probably clear the spawning status here too.
            if (workerId in spawningMap)
                spawningMap.remove(workerId);

            const uint imgIdx = pcb.activeImageIdx;
            if (imgIdx == uint.max)
            {
                managerLogger.errorf("Worker %d finished, but activeImageIdx was max.", workerId);
                pcb.state = WorkerState.IDLE;
                return true;
            }

            auto status = cast(TaskFinishCodes) msg.payload[0];
            processTaskFinish(pcb, imgIdx, status);

            pcb.state = WorkerState.IDLE;
            pcb.activeImageIdx = uint.max;
            pcb.taskTimer.stop();
            return true;

        case WorkerMessages.WorkerError:
        case WorkerMessages.MessageInvalid:
            managerLogger.errorf("Worker %d critical failure: %s", workerId, msg.msgType);
            pcb.state = WorkerState.UNRESPONSIVE;
            return true;

        case WorkerMessages.WorkerStop:
            pcb.state = WorkerState.KILLED;
            return true;

        default:
            // If we don't know what this is, we might as well discard it 
            // or return true so it doesn't clog the rotation.
            return true;
        }
    }

    private void processTaskFinish(ProcessControlBlock* pcb, uint imgIdx, TaskFinishCodes status)
    {
        final switch (status)
        {
        case TaskFinishCodes.Success:
            currentChunk.taskStates[imgIdx] = TaskState.COMPLETED;
            this.progCompleted++;
            break;

        case TaskFinishCodes.RetryRequested:
            if (currentChunk.retryCount[imgIdx] > 3)
            {
                currentChunk.taskStates[imgIdx] = TaskState.FAILED;
                this.progFailed++;
                managerLogger.errorf("Task %d:%d failed after max retries", currentChunk.chunkId, imgIdx);
                break;
            }
            currentChunk.retryCount[imgIdx]++;
            currentChunk.taskStates[imgIdx] = TaskState.PENDING;
            managerLogger.warningf("Task %d:%d retry %d/3",
                currentChunk.chunkId, imgIdx, currentChunk.retryCount[imgIdx]);

            break;

        case TaskFinishCodes.Failed:
            currentChunk.taskStates[imgIdx] = TaskState.FAILED;
            this.progFailed++;
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

    private void initiateNewWorker()
    {
        import std.random : uniform;

        uint tempId = uniform!uint;

        ProcessControlBlock pcb;
        pcb.pid = Pid.init;
        pcb.state = WorkerState.SPAWNING;
        pcb.activeChunkId = currentChunk.chunkId;
        pcb.activeImageIdx = uint.max;
        pcb.taskTimer = StopWatch(AutoStart.yes);
        pcb.lastMemoryUsage = 250 * 1024 * 1024; // 200 MB which is approx amount of barebones python runtime

        this.spawningMap[tempId] = WorkerCreationStatus.ReservingEndpoints;
        this.pcbMap[tempId] = pcb;

        this.herald.reserveEndpoints(tempId);
        managerLogger.infof("Initiated spawn for Chunk %d (Request ID: %d)", currentChunk.chunkId, tempId);
    }

    void updateSpawningWorkers()
    {
        foreach (uint id, ref WorkerCreationStatus status; this.spawningMap.dup)
        {
            final switch (status)
            {
            case WorkerCreationStatus.ReservingEndpoints:
                // Right now the ID we are working on the the TempID the initializeNewWorker initialized this worker with
                immutable ZMQEndpoints* msg = heraldMailbox.popFromBucket!(
                    immutable ZMQEndpoints)(id);
                if (msg == null)
                    break;
                if (msg.workerId != id)
                    break;

                // At this point, we have reserved some endpoints for our message
                // Now we can spawn a worker and set status to Registering
                // Spawning goes ReservingEndpoints --> endpoints are received, worker is spawned --> RegisteringConnection --> worker is registered to those endpoints, thus forming a connection object
                ZMQEndpoints endpoints = cast(ZMQEndpoints)*msg;

                try
                {
                    Pid pid = this.runner.spawn_worker(endpoints);
                    if (pid is Pid.init)
                    {
                        managerLogger.errorf("OS failed to spawn worker for Chunk %d",
                            currentChunk.chunkId);
                        herald.unreserveEndpoints(endpoints);
                        this.pcbMap.remove(id);
                        this.spawningMap.remove(id);
                        return;
                    }

                    // Now we get the real workerPid and perform the handoff
                    uint workerPid = cast(uint) pid.processID;

                    this.pcbMap[workerPid] = pcbMap[id];
                    this.pcbMap[workerPid].pid = pid;
                    this.pcbMap.remove(id);

                    this.spawningMap[workerPid] = WorkerCreationStatus.RegisteringConnection;
                    this.spawningMap.remove(id);

                    endpoints.workerId = workerPid;
                    herald.registerWorker(workerPid, endpoints);
                    managerLogger.infof("Worker %d spawned. ZMQ: IN=%s OUT=%s",
                        workerPid, endpoints.inEndpoint, endpoints.outEndpoint);
                }
                catch (Exception e)
                {
                    managerLogger.errorf("Critical failure spawning worker: %s", e.msg);
                    herald.unreserveEndpoints(endpoints);
                    this.pcbMap.remove(id);
                    this.spawningMap.remove(id);
                }

                break;

            case WorkerCreationStatus.RegisteringConnection:
                // Now we are working on the real process pid assigned by the OS
                M_RegisterResponse* res = heraldMailbox.popFromBucket!M_RegisterResponse(id);
                if (res is null)
                    break;

                if (res.response == false)
                    break; // I dont actually know what to do if register response failes yet, TODO later

                // Now that Process has been registered, we can set WorkerCreationStatus to created, and move on with our lives?
                spawningMap[id] = WorkerCreationStatus.Created;
                break;

            case WorkerCreationStatus.Created: // Skipped, the pcb state machine will use this as a gate and then promptly remove it from the spawningMap
                // I dont actually know what to do with these because these are not supposed to be set according to the codebase yet, TODO later
            case WorkerCreationStatus.UnreservingEndpoints:
            case WorkerCreationStatus.DeregisteringConnection:
                break;
            }
        }
    }

    private SpawnVerdict canSpawn(size_t chunkId)
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
        uint[] realPids;
        realPids.reserve(pcbMap.length);
        foreach (uint pid; pcbMap.keys)
        {
            if (pid !in spawningMap)
                realPids ~= pid;
        }

        this.monitor.updateTracking(realPids);
        this.monitor.refresh(monitorMailbox);
        this.monitorMap.clear();

        size_t total = 0;
        foreach (uint pid; this.pcbMap.keys)
        {
            ProcessMonitorReport report = ProcessMonitorReport(); // default initializes everything to 0

            // If a process is spawning currently, its RAM usage will be set to default value of 200 MB
            if (pid !in this.spawningMap)
            {
                report = this.monitor.getUsage(cast(uint) pid);
                this.monitorMap[pid] = report;
            }

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
                kill(pcbMap[pid].pid);
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

    public int[3] getProgressStats()
    {
        return [
            cast(int) this.progCompleted, cast(int) pcbMap.length,
            cast(int) this.progFailed
        ];
    }
}

public class ProcessRunner
{
    import std.process;
    import std.path;
    import std.file;
    import std.conv : to;
    import std.exception : enforce;
    import appstate;

    private string pythonPath;
    private string scriptPath;
    private string workflow;
    private string planPath;
    private string[string] baseEnv;

    this(string pythonPath, string scriptName, TaskMode mode, string planPath)
    {
        this.pythonPath = pythonPath;
        this.planPath = planPath.absolutePath();

        final switch (mode)
        {
        case TaskMode.ALIGN:
            this.workflow = "ALIGN";
            break;
        case TaskMode.TEST:
            this.workflow = "TEST";
            break;
        case TaskMode.TILING:
            this.workflow = "TILING";
            break;
        case TaskMode.MOCK:
            this.workflow = "MOCK";
            break;
        }

        this.scriptPath = buildPath(thisExePath().dirName(), "..", "engine", scriptName)
            .absolutePath();

        enforce(this.scriptPath.exists,
            "CRITICAL: Engine script missing at " ~ this.scriptPath);

        this.baseEnv = environment.toAA();
        this.baseEnv["OMP_NUM_THREADS"] = "1";
        this.baseEnv["MKL_NUM_THREADS"] = "1";
        this.baseEnv["OPENBLAS_NUM_THREADS"] = "1";
        this.baseEnv["VECLIB_MAXIMUM_THREADS"] = "1";
        this.baseEnv["OPENCV_FOR_THREADS_NUM"] = "1";

        this.baseEnv["OMSPEC_WORKFLOW"] = this.workflow;
        this.baseEnv["OMSPEC_PLANPATH"] = this.planPath;
        this.baseEnv["OMSPEC_LOGDIR"] = buildPath(planPath.dirName(), "log").absolutePath();

        if (!this.baseEnv["OMSPEC_LOGDIR"].exists)
            mkdirRecurse(this.baseEnv["OMSPEC_LOGDIR"]);

        managerLogger.infof("ProcessRunner initialized for %s. Script: %s",
            this.workflow, this.scriptPath);
    }

    public Pid spawn_worker(ZMQEndpoints endpoints)
    {
        try
        {
            string[string] workerEnv = this.baseEnv.dup;

            workerEnv["OMSPEC_INPUT_STREAM"] = endpoints.inEndpoint; // Where server PULLS
            workerEnv["OMSPEC_OUTPUT_STREAM"] = endpoints.outEndpoint; // Where server PUSHES

            managerLogger.infof("[%s] Spawning worker on streams IN: %s | OUT: %s",
                this.workflow,
                endpoints.inEndpoint,
                endpoints.outEndpoint);

            return spawnProcess(
                [this.pythonPath, this.scriptPath],
                workerEnv,
                Config.retainStderr | Config.retainStdout
            );
        }
        catch (Exception e)
        {
            managerLogger.errorf("OS Error during worker spawn: %s", e.msg);
            return Pid.init; // Returns an invalid Pid to signal failure
        }
    }
}
