module process_monitor_h;

import std.stdio;
import core.sys.windows.windows;
import core.sys.windows.psapi;
import std.datetime.stopwatch : StopWatch, AutoStart;
import core.time : msecs, Duration;

struct ProcessMonitorReport
{
    uint pid;
    ulong memory;
    double cpu;
}

struct M_TelemetryUpdate
{
    immutable uint[] pidUsageKeys;
    immutable ProcessMonitorReport[] pidUsageValues;
    ulong availableSystemRAM;
}

struct M_UpdatePidList
{
    immutable uint[] pids;
}

struct M_MonitorWorkerShutdown
{
}

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
    private ulong[uint] lastProcessTime;
    private ulong[uint] lastSystemTime;

    ulong getProcessMemory(uint pid)
    {
        HANDLE hProcess = OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, FALSE, pid);
        if (hProcess == null)
            return 0;
        scope (exit)
            CloseHandle(hProcess);

        PROCESS_MEMORY_COUNTERS_EX counters;
        counters.cb = counters.sizeof;

        // Use K32 version to ensure EX struct compatibility
        if (K32GetProcessMemoryInfo(hProcess, cast(PROCESS_MEMORY_COUNTERS*)&counters, counters
                .sizeof))
        {
            return cast(ulong) counters.PrivateUsage;
        }
        return 0;
    }

    /*  Returns CPU usage of one process in CPU-% as in %age of one equivalent CPU logical core
        Returns a value from 0 to 100*L where L is number of logical cores in system */
    ulong getProcessCPU(uint pid)
    {
        HANDLE hProcess = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, pid);
        if (hProcess == null)
            return 0;
        scope (exit)
            CloseHandle(hProcess);

        FILETIME creationTime, exitTime, kernelTime, userTime;
        FILETIME sysTime;
        GetSystemTimeAsFileTime(&sysTime);

        if (GetProcessTimes(hProcess, &creationTime, &exitTime, &kernelTime, &userTime))
        {
            ulong currentProcessTime = (
                cast(ulong) kernelTime.dwHighDateTime << 32 | kernelTime.dwLowDateTime
            ) + (
                cast(ulong) userTime.dwHighDateTime << 32 | userTime.dwLowDateTime
            );
            ulong currentSystemTime = (
                cast(ulong) sysTime.dwHighDateTime << 32 | sysTime.dwLowDateTime
            );

            ulong prevProc = lastProcessTime.get(pid, 0);
            ulong prevSys = lastSystemTime.get(pid, 0);

            lastProcessTime[pid] = currentProcessTime;
            lastSystemTime[pid] = currentSystemTime;

            if (prevSys == 0)
                return 0;

            ulong sysDelta = currentSystemTime - prevSys;
            ulong procDelta = currentProcessTime - prevProc;

            return (sysDelta > 0) ? (100 * procDelta) / sysDelta : 0;
        }
        return 0;
    }

    ProcessMonitorReport getProcessReport(uint pid)
    {
        ProcessMonitorReport report;
        report.pid = pid;
        report.memory = getProcessMemory(pid);
        report.cpu = cast(double) getProcessCPU(pid);
        return report;
    }

    ulong getSystemAvailablePhysical()
    {
        MEMORYSTATUSEX statex;
        statex.dwLength = statex.sizeof;
        if (GlobalMemoryStatusEx(&statex))
            return statex.ullAvailPhys;
        return 0;
    }
}

class MockMemoryFetcher : WindowsMemoryFetcher
{
    override ulong getProcessMemory(uint pid)
    {
        return 500 * 1024 * 1024;
    }
}

version (StandaloneTest)
{
    pragma(lib, "psapi.lib");
    pragma(lib, "kernel32.lib");

    void main()
    {
        auto fetcher = new WindowsMemoryFetcher();
        writeln("--- Hardware Telemetry Test ---");
        writeln("Enter PID to monitor:");

        uint targetPid;
        if (readf(" %d", &targetPid))
        {
            while (true)
            {
                auto report = fetcher.getProcessReport(targetPid);
                auto availableSystemRAM = fetcher.getSystemAvailablePhysical();

                if (report.memory == 0)
                {
                    writeln("Process not found or access denied.");
                    break;
                }

                writefln("PID: %d | RAM: %.2f MB / %.2f MB | CPU: %.1f%%",
                    report.pid,
                    cast(double) report.memory / (1024 * 1024),
                    cast(double) availableSystemRAM / (1024 * 1024),
                    report.cpu
                );

                import core.thread;

                Thread.sleep(1000.msecs);
            }
        }
    }
}

extern (Windows) BOOL K32GetProcessMemoryInfo(
    HANDLE Process,
    PROCESS_MEMORY_COUNTERS* ppsmemCounters,
    DWORD cb
);

struct PROCESS_MEMORY_COUNTERS_EX
{
    DWORD cb;
    DWORD PageFaultCount;
    SIZE_T PeakWorkingSetSize;
    SIZE_T WorkingSetSize;
    SIZE_T QuotaPeakPagedPoolUsage;
    SIZE_T QuotaPagedPoolUsage;
    SIZE_T QuotaPeakNonPagedPoolUsage;
    SIZE_T QuotaNonPagedPoolUsage;
    SIZE_T PagefileUsage;
    SIZE_T PeakPagefileUsage;
    SIZE_T PrivateUsage;
}
