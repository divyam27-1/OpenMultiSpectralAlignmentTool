import core.thread;
import core.time;

import std.stdio;
import std.getopt;
import std.file;
import std.path;
import std.conv : to;
import std.json;
import std.algorithm.iteration;

import appstate;
import planning;
import planning_h;
import process_control;
import process_control_h;
import config;
import usage_tracker;

void main(string[] args)
{
    // Load the config
    auto configPath = buildPath(thisExePath().dirName, "omspec.cfg");
    Config cfg = loadConfig(configPath);

    // Route Ctrl-C to handleInterrupt
    import core.stdc.signal;
    signal(SIGINT, &handleInterrupt);

    // Default values
    string target = getcwd();
    int maxDepth = cfg.scan_max_depth;
    bool alignMode = false;
    bool testMode = false;
    bool tilingMode = false;

    // CLI Argument Parsing
    auto helpInformation = getopt(
        args,
        "align", "Align multispectral bands (Default)", &alignMode,
        "test", "Run alignment testbench", &testMode,
        "tiling", "Perform image tiling for ML", &tilingMode,
        "target|i", "Target directory (Default: PWD)", &target,
        "depth", "Max tree depth to scan (Default: 3)", &maxDepth
    );

    if (helpInformation.helpWanted)
    {
        defaultGetoptPrinter("omspec - Open Multispectral Alignment Tool", helpInformation.options);
        writeln();
        writeln("Configuration Path: ", configPath);
        writeln(cfg.toString());
        return;
    }

    TaskMode mode = alignMode ? TaskMode.ALIGN : (testMode ? TaskMode.TEST
            : (tilingMode ? TaskMode.TILING : TaskMode.MOCK));

    writeln("--- Open Multispectral Alignment Tool (omspec) ---");
    version (release)
    {
    }
    else
    {
        writeln("Debug Build at ", thisExePath());
    }
    writeln("Mode:   ", mode);
    writeln("Target: ", target);
    writeln("Depth:  ", maxDepth);
    writeln("License:  Community Edition (Non-Commercial User Only)");
    writeln("--------------------------------------------------");

    setTargetPath(absolutePath(target));
    writeln("Target Path set to: ", targetPath);

    writeln("Generating Process Plan...");

    DatasetChunk[] plan = generate_plan(target, maxDepth);

    writeln("\nTotal Chunks to process: ", plan.length);

    size_t plan_size = reduce!((a, s) => a + s.chunk_size)(0UL, plan);
    UsageTracker usageTracker = new UsageTracker();
    if (!usageTracker.incrementAndCheck(plan_size))
        return;

    string planOutputPath = buildPath(target, "plan.json").absolutePath();
    save_plan_to_json(plan, planOutputPath);

    writeln("Ready to Spawn Workers.");
    writeln("--------------------------------------------------");

    writeln("Spawning Workers...\n");

    string python_path = buildPath(thisExePath().dirName(), "..", "python_3_11_14", "install", "python.exe")
        .absolutePath();

    ProcessRunner runner = new ProcessRunner(mode, python_path);
    Scheduler controller = new Scheduler(runner, planOutputPath, 150);

    bool ret = controller.execute_plan();

    writeln("--------------------------------------------------");
    if (ret)
        writeln("All Tasks Finished Successfully");
    else
        controller.print_summary();
    writeln("--------------------------------------------------");
}
