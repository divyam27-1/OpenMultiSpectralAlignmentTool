module process_monitor;

import std.stdio;
import core.sys.windows.windows;
import core.sys.windows.psapi;
import std.datetime : Duration;
import std.datetime.stopwatch : StopWatch, AutoStart;
import core.time : msecs, Duration;
import std.concurrency;
import core.thread;

import process_monitor_h;

class ProcessMonitor
{
    private ProcessMonitorReport[uint] pidUsageMap; // Track memory per PID
    private ulong systemRAMUsage;
    private Tid monitorTid;

    private StopWatch sw = StopWatch(AutoStart.no);
    private const Duration refreshInterval = 500.msecs;

    this(IMemoryFetcher fetcher)
    {
        this.monitorTid = spawn(&monitorWorker, thisTid, cast(shared)fetcher);
    }

    // Refreshes the monitors local state
    void refresh()
    {
        M_TelemetryUpdate latest;
        bool foundNew = false;

        // Keep pulling until the mailbox is EMPTY
        while (receiveTimeout(dur!"msecs"(0), 
            (M_TelemetryUpdate update) {
                latest = update; // Overwrite with newer data
                foundNew = true;
            }))
        {}

        if (foundNew)
        {
            this.pidUsageMap = latest.pidUsageMap;
            this.systemRAMUsage = latest.availableSystemRAM;
        }
    }

    void updateTracking(uint[] pids)
    {
        send(monitorTid, M_UpdatePidList(pids));
    }

    ProcessMonitorReport getUsage(uint pid)
    {
        return pidUsageMap.get(pid, ProcessMonitorReport.init);
    }

    ulong getAvailableSystemRAM()
    {
        return systemRAMUsage;
    }
}

void monitorWorker(Tid parentTid, shared IMemoryFetcher sFetcher)
{
    auto fetcher = cast(IMemoryFetcher) sFetcher;
    uint[] pidsToTrack;
    auto sw = StopWatch(AutoStart.yes);

    bool spin = true;

    while (spin)
    {
        receiveTimeout(dur!"msecs"(10),
            (M_UpdatePidList msg) { pidsToTrack = msg.pids; },
            (M_MonitorWorkerShutdown msg) { spin = false; }
        );

        if (sw.peek() < 500.msecs)
        {
            Thread.sleep(50.msecs);
            continue;
        }

        M_TelemetryUpdate report;

        foreach (pid; pidsToTrack)
        {
            report.pidUsageMap[pid] = fetcher.getProcessReport(pid);
        }
        report.availableSystemRAM = fetcher.getSystemAvailablePhysical();

        send(parentTid, report);
        sw.reset();
    }
}
