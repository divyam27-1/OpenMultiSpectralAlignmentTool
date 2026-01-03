module process_monitor;

import std.stdio;
import core.sys.windows.windows;
import core.sys.windows.psapi;
import std.datetime;

import process_monitor_h;

// The Wrapper that the rest of the App uses
class ProcessMonitor {
    private IMemoryFetcher fetcher;
    private ulong[uint] pidUsageMap; // Track memory per PID

    private MonoTime lastRefresh;
    private const refreshInterval = msecs(500);

    this(IMemoryFetcher fetcher) {
        this.fetcher = fetcher;
        this.lastRefresh = MonoTime.currTime();
    }

    void refresh(ref uint[uint] pcbMap, bool force=false) {
        auto now = MonoTime.currtime();
        if (!force && (now - lastRefresh) < refreshInterval) return;

        pidUsageMap.clear();
        foreach (pid; pcbMap.byKey) {
            pidUsageMap[pid] = fetcher.getProcessMemory(pid);
        }

        lastRefresh = now;
    }

    ulong getUsage(uint pid) {
        return pidUsageMap.get(pid, 0);
    }

    ulong getAvailableSystemRAM() {
        return fetcher.getSystemAvailablePhysical();
    }
}