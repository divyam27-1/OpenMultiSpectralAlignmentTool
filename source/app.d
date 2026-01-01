import core.thread;
import core.time;

import std.stdio;
import std.getopt;
import std.file;
import std.path;
import std.conv : to;
import std.json;

import omspec_ipc;
import planning;
import process_control;

void main(string[] args)
{
    // Default values
    string target = getcwd();
    int maxDepth = 3;
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
        return;
    }

    // Default to align if no mode is 
    // TODO: after implementing actual python workers uncomment this
    // if (!testMode && !tilingMode) alignMode = true;

    TaskMode mode = alignMode ? TaskMode.ALIGN : (testMode ? TaskMode.TEST
            : (tilingMode ? TaskMode.TILING : TaskMode.MOCK));

    writeln("--- Open Multispectral Alignment Tool (omspec) ---");
    writeln("Mode:   ", mode);
    writeln("Target: ", target);
    writeln("Depth:  ", maxDepth);
    writeln("--------------------------------------------------");

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
    ProcessController controller = get_runner(mode, python_path);

    controller.execute_plan(planOutputPath);

    writeln("--------------------------------------------------");
}
