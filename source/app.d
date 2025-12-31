import core.thread;
import core.time;

import std.stdio;
import std.getopt;
import std.file;

import omspec_ipc;
import planning;

void main(string[] args) {
    // Default values
    string target = getcwd();
    int maxDepth = 3;
    bool alignMode = false;
    bool testMode = false;
    bool tilingMode = false;

    // CLI Argument Parsing
    auto helpInformation = getopt(
        args,
        "align",  	"Align multispectral bands (Default)", 	&alignMode,
        "test",   	"Run alignment testbench", 				&testMode,
        "tiling", 	"Perform image tiling for ML", 			&tilingMode,
        "target|i", "Target directory (Default: PWD)", 		&target,
        "depth",  	"Max tree depth to scan (Default: 3)", 	&maxDepth
    );

    if (helpInformation.helpWanted) {
        defaultGetoptPrinter("omspec - Open Multispectral Alignment Tool", helpInformation.options);
        return;
    }

    // Default to align if no mode is specified
    if (!testMode && !tilingMode) alignMode = true;

    string modeStr = testMode ? "TESTING" : (tilingMode ? "TILING" : "ALIGNMENT");

    writeln("--- Open Multispectral Alignment Tool (omspec) ---");
    writeln("Mode:   ", modeStr);
    writeln("Target: ", target);
    writeln("Depth:  ", maxDepth);
    writeln("--------------------------------------------------");

	Thread.sleep(dur!"msecs"(250));

	writeln("Generating Process Plan...");

    DatasetChunk[] plan = generate_plan(target, maxDepth);

    writeln("\nTotal Chunks to process: ", plan.length);

    string planOutputPath = target ~ "/plan.json";
    save_plan_to_json(plan, planOutputPath);

    writeln("Ready to Spawn Workers.");
	writeln("--------------------------------------------------");
}