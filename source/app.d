import core.thread;
import core.time;

import std.stdio;
import std.getopt;
import std.file;
import std.path;
import std.conv : to;
import std.json;
import std.algorithm.iteration;
import std.datetime.systime : SysTime, Clock;
import std.datetime.stopwatch : StopWatch, AutoStart;
import std.string;
import std.logger;

import appstate;
import planning;
import planning_h;
import process_control;
import process_manager;
import config;
import usage_tracker;
import loggers;

void main(string[] args)
{
    // Load the config
    auto configPath = buildPath(thisExePath().dirName, "omspec.cfg");
    Config cfg = loadConfig(configPath);
    SysTime currentTime = Clock.currTime();
    string currentTimeString = currentTime.toISOExtString().replace(":", "-");
    StopWatch sw = StopWatch(AutoStart.no);

    // Route Ctrl-C to handleInterrupt
    import core.stdc.signal;

    signal(SIGINT, &handleInterrupt);

    // Default values
    string target = getcwd();
    int maxDepth = cfg.scan_max_depth;
    bool alignMode = false;
    bool testMode = false;
    bool tilingMode = false;
    bool benchmarkMode = false;

    // CLI Argument Parsing
    auto helpInformation = getopt(
        args,
        "align", "Align multispectral bands (Default)", &alignMode,
        "test", "Run alignment testbench", &testMode,
        "tiling", "Perform image tiling for ML", &tilingMode,
        "target|i", "Target directory (Default: PWD)", &target,
        "depth", "Max tree depth to scan (Default: 3)", &maxDepth,
        "benchmark|b", "Benchmark Mode (Tracks Execution Time)", &benchmarkMode,
    );

    if (helpInformation.helpWanted)
    {
        defaultGetoptPrinter("omspec - Open Multispectral Alignment Tool", helpInformation.options);
        writeln();
        writeln("Configuration Path: ", configPath);
        writeln(cfg.toString());
        return;
    }

    mode = alignMode ? TaskMode.ALIGN : (testMode ? TaskMode.TEST : (tilingMode ? TaskMode.TILING
            : TaskMode.MOCK));

    writeln("--- Open Multispectral Alignment Tool (omspec) ---");
    version (release)
    {
    }
    else
        writeln("Debug Build at ", thisExePath());

    writeln("Mode:   ", mode);
    writeln("Target: ", target);
    writeln("Depth:  ", maxDepth);
    writeln("License:  Community Edition (Non-Commercial User Only)");
    if (benchmarkMode)
        writeln("Benchmark Mode: Enabled");
    writeln("--------------------------------------------------");

    sw.start();

    setTargetPath(absolutePath(target));
    writeln("Target Path set to: ", targetPath);

    string logDir = buildPath(target, "log");
    if (!exists(logDir))
        mkdir(logDir);

    mainLogger = new FileLogger(buildPath(logDir, "main_" ~ currentTimeString ~ ".log"),
        LogLevel.info, CreateFolder.yes);
    planLogger = new FileLogger(buildPath(logDir, "plan_" ~ currentTimeString ~ ".log"),
        LogLevel.info, CreateFolder.yes);
    processLogger = new FileLogger(buildPath(logDir, "process_" ~ currentTimeString ~ ".log"),
        LogLevel.info, CreateFolder.yes);
    monitorLogger = new FileLogger(buildPath(logDir, "monitor_" ~ currentTimeString ~ ".log"),
        LogLevel.info, CreateFolder.yes);
    managerLogger = new FileLogger(buildPath(logDir, "manager_" ~ currentTimeString ~ ".log"),
        LogLevel.info, CreateFolder.yes);
    workerLogPath = buildPath(logDir, "worker_%u_" ~ currentTimeString ~ ".log");

    writeln("Generating Process Plan...\n");

    DatasetChunk[] plan = generate_plan(target, maxDepth);

    size_t plan_size = reduce!((a, s) => a + s.chunk_size)(0UL, plan);
    UsageLimitTracker UsageLimitTracker = new UsageLimitTracker();
    if (!UsageLimitTracker.incrementAndCheck(plan_size))
        return;

    string planOutputPath = buildPath(target, "plan.json").absolutePath();
    save_plan_to_json(plan, planOutputPath);
    mainLogger.infof("Generated Process Plan at %s", planOutputPath);

    writeln("\nReady to Spawn Workers.");
    writeln("--------------------------------------------------");

    writeln("Spawning Workers...\n");

    string python_path = buildPath(thisExePath().dirName(), "..", "python_3_11_14", "install", "python.exe")
        .absolutePath();
    mainLogger.infof("Internal Python Runtime Path: %s", python_path);

    ProcessRunner runner = new ProcessRunner(python_path, "worker.py", mode, planOutputPath);
    Scheduler controller = new Scheduler(runner, planOutputPath);

    bool ret = controller.execute_plan();

    writeln("--------------------------------------------------");

    string exitStatement = ret ? "All tasks finished successfully" : controller.get_summary();
    string benchmarkStatement = benchmarkMode ? format("Time taken: %s", sw.peek().toString()) : "";

    writeln(exitStatement);
    if (benchmarkMode)
        writeln(benchmarkStatement);
    writeln("--------------------------------------------------");
    mainLogger.info("Finished");
}
