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

    private int i = 0;

    this(ProcessRunner runner, string planPath)
    {
        this.planPath = planPath;
        this.manager = new ProcessManager(runner);

        auto cfg = loadConfig(buildPath(thisExePath().dirName, "omspec.cfg").absolutePath());
        this.tickIntervalMS = cfg.tick_interval_ms;
    }

    public bool execute_plan()
    {
        // Read plan from JSON
        auto plan = parseJSON(readText(this.planPath));

        // On exit, terminate all workers
        scope (exit)
        {
            this.manager.terminateAll();
            progBar.finish();
        }

        size_t num_chunks = plan.array.length;
        if (num_chunks <= 0)
        {
            processLogger.errorf("Plan is empty");
            return false;
        }

        size_t[] chunk_sizes = plan.array.map!(c => cast(size_t) c["chunk_size"].get!long).array;

        foreach (i; 0 .. num_chunks)
            this.taskQueue.insertBack(i);
        
        size_t queueSize = this.taskQueue[].walkLength;

        // Spawn progress bar
        progBar = ProgressBar(cast(int) num_chunks);

        while (!this.taskQueue.empty || running > 0)
        {
            if (atomicLoad(shutdownRequested))
            {
                writeln(
                    "Shutdown requested by user. Stopping scheduler and Terminating all workers..."
                );
                processLogger.warning("Shutdown requested by user. Stopping scheduler...");
                break; // Exit the loop to trigger the scope(exit) cleanup
            }

            // Phase 1 : Update ProgressBar values (Cleanup is handled by Manager)
            ChunkGraveyard graveyard = this.manager.reap();
            foreach (uint i; graveyard.completed)
                completed++;
            foreach (uint i; graveyard.failed)
                failed++;
            foreach (uint i; graveyard.retries)
                this.taskQueue.insertFront(i);
            running = cast(int) this.manager.getActiveProcessCount();

            // Phase 2 : Worker Spawning (Backfill Aware)
            size_t checkedCount = 0;
            while (!this.taskQueue.empty && checkedCount < queueSize)
            {
                size_t next_chunk_id = this.taskQueue.front;
                size_t next_chunk_size = chunk_sizes[next_chunk_id];

                SpawnVerdict result = this.manager.trySpawn(next_chunk_id, next_chunk_size);

                if (result == SpawnVerdict.OK)
                {
                    this.taskQueue.removeFront();
                    queueSize--;    // defined when queue is first loaded
                    checkedCount = 0;
                    continue;
                }

                if (result == SpawnVerdict.SYSTEM_BUSY_CPU)
                    break;

                // Memory Busy (Theoretical or System RAM): Try to find a smaller chunk.
                if (result == SpawnVerdict.SYSTEM_BUSY_RAM || result == SpawnVerdict
                    .LIMIT_REACHED_MEM)
                {
                    this.taskQueue.removeFront();
                    this.taskQueue.insertBack(next_chunk_id);
                    checkedCount++; // Increment so we don't loop forever
                    continue;
                }

                if (result == SpawnVerdict.SPAWN_FAILURE)
                    break;
            }

            // Phase 3: Tick Wait
            progBar.update(completed, running, failed);
            // i have removed logging resource usage here. it will be logged in process monitor or process manager
            Thread.sleep(dur!"msecs"(tickIntervalMS));
        }

        progBar.finish();
        return failed == 0;
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
