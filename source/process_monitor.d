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
    private ulong availableSystemRAM;
    private Tid monitorTid;

    private StopWatch sw = StopWatch(AutoStart.no);
    private const Duration refreshInterval = 500.msecs;

    this(IMemoryFetcher fetcher)
    {
        this.monitorTid = spawn(&monitorWorker, thisTid, cast(shared) fetcher);
    }

    // Refreshes the monitors local state
    void refresh()
    {
        M_TelemetryUpdate* latest = null;

        // Keep pulling until the mailbox is EMPTY
        while (receiveTimeout(dur!"msecs"(0),
                (M_TelemetryUpdate update) {
                latest = new M_TelemetryUpdate(update.pidUsageKeys, update.pidUsageValues, update
                .availableSystemRAM); // Overwrite with newer data
            }))
        {
        }

        if (latest !is null)
        {
            this.pidUsageMap.clear();
            foreach (size_t i, uint key; latest.pidUsageKeys)
                this.pidUsageMap[key] = latest.pidUsageValues[i];
            this.availableSystemRAM = latest.availableSystemRAM;
        }
    }

    void updateTracking(uint[] pids)
    {
        send(monitorTid, M_UpdatePidList(pids.idup));
    }

    ProcessMonitorReport getUsage(uint pid)
    {
        return pidUsageMap.get(pid, ProcessMonitorReport.init);
    }

    ulong getAvailableSystemRAM()
    {
        return availableSystemRAM;
    }

    void shutDownWorker()
    {
        send(monitorTid, M_MonitorWorkerShutdown());
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
        try
        {
            receiveTimeout(dur!"msecs"(10),
                (M_UpdatePidList msg) { pidsToTrack = msg.pids.dup; },
                (M_MonitorWorkerShutdown msg) { spin = false; }
            );
        }
        catch (OwnerTerminated e)
            spin = false;

        if (sw.peek() < 500.msecs)
        {
            Thread.sleep(50.msecs);
            continue;
        }

        uint[] reportKeys;
        ProcessMonitorReport[] reportValues;

        foreach (pid; pidsToTrack)
        {
            reportKeys ~= pid;
            reportValues ~= fetcher.getProcessReport(pid);
        }

        M_TelemetryUpdate report = M_TelemetryUpdate(
            reportKeys.idup, reportValues.idup, fetcher.getSystemAvailablePhysical()
        );

        send(parentTid, report);
        sw.reset();
    }
}
