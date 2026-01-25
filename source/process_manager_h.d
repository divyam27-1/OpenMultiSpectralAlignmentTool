module process_manager_h;

import std.stdio;
import std.parallelism : totalCPUs;
import core.sys.windows.windef; // For DWORD, etc.
import core.stdc.stdlib : malloc, free;
import std.string;
import std.process : Pid;
import std.datetime.systime : SysTime;
import std.datetime.stopwatch : StopWatch, AutoStart;
import std.conv : to;
import std.container.dlist;
import std.variant;

enum WorkerState
{
    SPAWNING, // OS process created, Herald hasn't seen it yet
    IDLE, // Connected and waiting for work
    BUSY, // Executing a task
    FLUSHING, // Executing a disk write
    UNRESPONSIVE, // Missed heartbeats, marked for death
    KILLED, // Process has gracefully exited and is soon to be cleaned up
}

struct ProcessControlBlock
{
    Pid pid;
    uint workerId;
    WorkerState state = WorkerState.SPAWNING;

    // Telemetry
    size_t lastMemoryUsage;

    // Current Work Context (Null if IDLE)
    uint activeChunkId;
    uint activeImageIdx = uint.max;
    StopWatch taskTimer;
}

enum TaskState
{
    PENDING,
    INPROGRESS,
    COMPLETED,
    FAILED,
}

struct ChunkControlBlock
{
    uint chunkId;
    size_t numImages;
    TaskState[] taskStates; // key: imageIdx, value: TaskState;
    uint[] retryCount; // key: imageIdx, value: number of retries

    this(uint chunkId, uint numImages)
    {
        this.chunkId = chunkId;
        this.numImages = numImages;

        for (size_t i = 0; i < numImages; i++)
        {
            taskStates ~= TaskState.PENDING;
            retryCount ~= 0;
        }
    }

    uint findNextPendingImage()
    {
        foreach (size_t i, TaskState state; taskStates)
            if (state == TaskState.PENDING)
                return cast(uint) i;

        return uint.max;
    }

    bool isComplete()
    {
        foreach (state; taskStates)
            if (state == TaskState.PENDING || state == TaskState.INPROGRESS)
                return false;

        return true;
    }

    bool hasUnassignedWork()
    {
        foreach (state; taskStates)
            if (state == TaskState.PENDING)
                return true;

        return false;
    }
}

public enum SpawnVerdict
{
    OK,
    SYSTEM_BUSY_CPU,
    SYSTEM_BUSY_RAM,
    LIMIT_REACHED_MEM,
    SPAWN_FAILURE
}

T* popFromBucket(T)(ref DList!Variant bucket, uint targetId)
{
    Variant[] range = bucket[];

    for (auto i = 0; i < range.length; i++)
    {
        Variant v = range[i];
        if (auto msg = v.peek!T)
        {
            uint msgId = 0;

            static if (__traits(hasMember, T, "workerId"))
            {
                msgId = msg.workerId;
            }
            else static if (__traits(hasMember, T, "tempWorkerID"))
            {
                msgId = msg.tempWorkerID;
            }
            else
            {
                static assert(false,
                    "Type " ~ T.stringof ~ " must have workerId or tempWorkerID to be used in popFromBucket");
            }

            if (msgId == targetId)
            {
                T* heapCopy = new T(*msg);
                bucket.linearRemove(range[i .. i + 1]);
                return heapCopy;
            }
        }
    }
    return null;
}

uint getPhysicalCoreCount()
{
    DWORD returnLength = 0;

    // First call to determine buffer size
    GetLogicalProcessorInformation(null, &returnLength);

    if (returnLength == 0)
        return 1;

    // Allocate buffer
    auto buffer = cast(SYSTEM_LOGICAL_PROCESSOR_INFORMATION*) malloc(returnLength);
    if (buffer is null)
        return 1;
    scope (exit)
        free(buffer);

    // Second call to get actual data
    if (!GetLogicalProcessorInformation(buffer, &returnLength))
    {
        return 1;
    }

    uint count = 0;
    size_t numEntries = returnLength / SYSTEM_LOGICAL_PROCESSOR_INFORMATION.sizeof;

    for (size_t i = 0; i < numEntries; i++)
    {
        // We only care about entries that represent a physical core
        if (buffer[i].Relationship == LOGICAL_PROCESSOR_RELATIONSHIP.RelationProcessorCore)
        {
            count++;
        }
    }

    return count > 0 ? count : 1;
}

// Manually defining missing Win32 bindings
enum LOGICAL_PROCESSOR_RELATIONSHIP
{
    RelationProcessorCore,
    RelationNumaNode,
    RelationCache,
    RelationProcessorPackage,
    RelationGroup,
    RelationAll = 0xffff
}

struct SYSTEM_LOGICAL_PROCESSOR_INFORMATION
{
    size_t ProcessorMask;
    LOGICAL_PROCESSOR_RELATIONSHIP Relationship;
    union
    {
        struct
        {
            ubyte Flags;
        } // ProcessorCore
        struct
        {
            ubyte NodeNumber;
        } // NumaNode
        struct
        {
            ubyte Level;
            ubyte Associativity;
            ushort LineSize;
            uint Size;
            uint Type; // CACHE_DESCRIPTOR
        } // Cache
        ulong[2] Reserved;
    }
}

extern (Windows)
{
    BOOL GetLogicalProcessorInformation(
        SYSTEM_LOGICAL_PROCESSOR_INFORMATION* Buffer,
        DWORD* ReturnedLength
    );
}

version (StandaloneTest)
{
    pragma(lib, "kernel32.lib");
    pragma(lib, "psapi.lib");

    void main()
    {
        uint pCores = getPhysicalCoreCount();
        writeln("--- CPU Topology ---");
        writeln("Physical Cores: ", pCores);
        writeln("Logical Cores:  ", totalCPUs);

        if (pCores == totalCPUs)
        {
            writeln("Note: Hyper-threading is likely disabled or not supported.");
        }
        else
        {
            writeln("Note: Hyper-threading detected.");
        }
    }
}
