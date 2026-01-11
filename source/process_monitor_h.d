module process_monitor_h;

struct ProcessMonitorReport {
    uint pid;
    size_t memory;
    float cpu;
}

struct M_TelemetryUpdate {
    ProcessMonitorReport[uint] pidUsageMap;
    ulong availableSystemRAM;
}

struct M_UpdatePidList {
    uint[] pids;
}

struct M_MonitorWorkerShutdown {}

// Used for multiple fetchers in the future, Windows, Linux, OSX etc.
interface IMemoryFetcher
{
    ulong getProcessMemory(uint pid);
    ulong getProcessCPU(uint pid);
    ProcessMonitorReport getProcessReport(uint pid);
    ulong getSystemAvailablePhysical();
}

class WindowsMemoryFetcher : IMemoryFetcher
{
    ulong getProcessMemory(uint pid)
    {
        HANDLE hProcess = OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, FALSE, pid);
        if (hProcess == null)
            return 0;

        PROCESS_MEMORY_COUNTERS counters;
        if (GetProcessMemoryInfo(hProcess, &counters, counters.sizeof))
        {
            CloseHandle(hProcess);
            return cast(ulong) counters.WorkingSetSize; // RAM currently in use
        }

        CloseHandle(hProcess);
        return 0;
    }

    ulong getSystemAvailablePhysical()
    {
        MEMORYSTATUSEX statex;
        statex.dwLength = statex.sizeof;
        GlobalMemoryStatusEx(&statex);
        return statex.ullAvailPhys;
    }
}

class MockMemoryFetcher : WindowsMemoryFetcher // mock should use the default windows for available memory as i am developing on windows
{
    override ulong getProcessMemory(uint pid)
    {
        return 500 * 1024 * 1024; // 500 MB
    }
}
