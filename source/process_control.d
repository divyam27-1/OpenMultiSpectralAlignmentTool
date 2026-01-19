module process_control;

import core.thread;
import core.time;
import core.atomic;
import core.stdc.signal;

import std.stdio;
import std.process;
import std.json;
import std.file;
import std.conv : to;
import std.array;
import std.algorithm;
import std.container.dlist;
import std.math : pow;
import std.path;
import std.datetime.systime : SysTime, Clock;
import std.logger;
import std.exception : enforce;
import std.range : walkLength;

import process_manager_h;
import process_manager;
import appstate;
import config;
import tui_h : ProgressBar;
import loggers : processLogger;

shared bool shutdownRequested = false;

public class Scheduler
{
    private ProcessManager manager;

    private string planPath;
    private DList!size_t taskQueue;

    // initialized to default values just in case something goes wrong with reading cfg
    private int tickIntervalMS = 75;

    // generats TUI and progress bar
    private ProgressBar progBar;
    private int completed = 0, running = 0, failed = 0;

    this(ProcessRunner runner, string planPath)
    {
        this.planPath = planPath;
        this.manager = new ProcessManager(runner);

        auto cfg = loadConfig(buildPath(thisExePath().dirName, "omspec.cfg").absolutePath());
        this.tickIntervalMS = cfg.tick_interval_ms;
    }

    public bool execute_plan()
    {
        JSONValue plan = parseJSON(readText(this.planPath));

        scope (exit)
        {
            this.manager.terminateAll();
            progBar.finish();
        }
        auto planArray = plan.array;
        size_t numChunks = planArray.length;
        size_t[] numImages;
        numImages.length = numChunks;

        // Initialize Queue
        foreach (i; 0 .. numChunks)
        {
            this.taskQueue.insertBack(cast(uint) i);
            numImages[i] = planArray[i]["images"].array.length;
        }

        processLogger.infof("Starting Execution of %d Chunks", numChunks);
        progBar = ProgressBar(cast(int) numChunks);

        while (!this.taskQueue.empty || this.manager.busy)
        {
            if (atomicLoad(shutdownRequested))
                break;

            // Phase 1: Feed the Manager
            if (!this.taskQueue.empty && !this.manager.busy)
            {
                uint next_chunk_id = cast(uint) this.taskQueue.front;
                uint num_images = cast(uint) numImages[next_chunk_id];
                this.taskQueue.removeFront();
                this.manager.putChunk(next_chunk_id, num_images);

                processLogger.infof("Put chunk %d with %d tasks in Manager", next_chunk_id, num_images);
            }

            // Phase 2: Run the Manager's "Timeslice"
            this.manager.processTick();

            // Phase 3: UI
            auto stats = this.manager.getProgressStats();
            completed = stats[0]; running = stats[1]; failed = stats[2];
            progBar.update(completed, running, failed);

            Thread.sleep(dur!"msecs"(tickIntervalMS));
        }

        return true;
    }

    public string get_summary()
    {
        import std.format;

        string line1 = format("Final Status: %d Success, %d Failed %d Total",
            this.completed, this.failed, this.completed + this.failed);

        if (this.failed > 0)
        {
            line1 ~= "\nCheck logs/process_ and logs/worker_ for specific error details on failed chunks.";
        }

        return line1;
    }
}

extern (C) void handleInterrupt(int sig) nothrow @nogc @system
{
    import core.atomic : atomicStore;

    atomicStore(shutdownRequested, true);
}
