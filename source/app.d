import core.thread;
import core.time;

import std.stdio;
import std.getopt;
import std.file;
import std.path;
import std.conv : to;
import std.json;

import appstate;
import omspec_ipc;
import planning;
import process_control;
import config;

void main(string[] args)
{
    // Load the config
    auto configPath = buildPath(thisExePath().dirName, "omspec.cfg");
    Config cfg = loadConfig(configPath);

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
    writeln("Mode:   ", mode);
    writeln("Target: ", target);
    writeln("Depth:  ", maxDepth);
    writeln("--------------------------------------------------");

    setTargetPath(absolutePath(target));
    writeln("Target Path set to: ", targetPath);

    writeln("Generating Process Plan...");

    DatasetChunk[] plan = generate_plan(target, maxDepth);

    writeln("\nTotal Chunks to process: ", plan.length);

    string planOutputPath = buildPath(target, "plan.json").absolutePath();
    save_plan_to_json(plan, planOutputPath);

    writeln("Ready to Spawn Workers.");
    writeln("--------------------------------------------------");

    writeln("Spawning Workers...");

    string python_path = buildPath(thisExePath().dirName(), "..", "python_3_11_14", "install", "python.exe")
        .absolutePath();

    ProcessRunner runner = new ProcessRunner(mode, python_path);
    Scheduler controller = new Scheduler(runner, planOutputPath, 150);

    controller.execute_plan();

    writeln("\nAll tasks completed.");
    writeln("--------------------------------------------------");
}
