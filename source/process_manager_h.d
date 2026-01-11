module process_manager_h;

import std.stdio;
import std.parallelism : totalCPUs;
import core.sys.windows.windef; // For DWORD, etc.
import core.stdc.stdlib : malloc, free;
import std.string;
import std.process : Pid;
import std.datetime.systime : SysTime;

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

struct ProcessResult
{
    size_t chunkId;
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
