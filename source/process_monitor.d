module process_monitor;

import std.stdio;
import core.sys.windows.windows;
import core.sys.windows.psapi;
import std.datetime : Duration;
import std.datetime.stopwatch : StopWatch, AutoStart;
import core.time : msecs, Duration;

// The Wrapper that the rest of the App uses
class ProcessMonitor {
    private IMemoryFetcher fetcher;
    private ulong[uint] pidUsageMap; // Track memory per PID

    private StopWatch sw = StopWatch(AutoStart.no);
    private const Duration refreshInterval = 500.msecs;

    this(IMemoryFetcher fetcher) {
        this.fetcher = fetcher;
        this.sw.start();
    }

    void refresh(ref uint[uint] pcbMap, bool force=false) {
        if (!force && (sw.peek() < refreshInterval)) return;

        pidUsageMap.clear();
        foreach (pid; pcbMap.byKey) {
            pidUsageMap[pid] = fetcher.getProcessMemory(pid);
        }

        sw.reset();
    }

    ulong getUsage(uint pid) {
        return pidUsageMap.get(pid, 0);
    }

    ulong getAvailableSystemRAM() {
        return fetcher.getSystemAvailablePhysical();
    }
}

interface IMemoryFetcher
{
    ulong getProcessMemory(uint pid);
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

class MockMemoryFetcher : WindowsMemoryFetcher      // mock should use the default windows for available memory as i am developing on windows
{
    override ulong getProcessMemory(uint pid) {
        return 500 * 1024 * 1024;       // 500 MB
    }
}
