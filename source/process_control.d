module process_control;

import core.thread;
import core.time;
import core.atomic : atomicOp;

import std.stdio;
import std.process;
import std.json;
import std.file;
import std.conv : to;
import std.array;
import std.math : pow;
import std.path;
import std.datetime.systime : SysTime, Clock;
import std.logger;
import std.exception : enforce;

SysTime current_time;
string log_filename;
FileLogger fileLogger;

static this() {
    if (!exists("log"))    mkdir("log");
    current_time = Clock.currTime();
    log_filename = "log\\processing_" ~ current_time.toISOExtString().replace(":", "-") ~ ".log";
    fileLogger = new FileLogger(log_filename, LogLevel.info, CreateFolder.yes);
}

enum TaskMode {
    MOCK,
    ALIGN,
    TEST,
    TILING
}

struct ProcessResult {
    size_t chunk_id;
    bool success;
    int exitCode;
}

public class ProcessController {
    private string pythonPath;
    private string scriptPath;

    this(string pyPath, string sPath) {
        this.pythonPath = pyPath;
        this.scriptPath = sPath;
    }

    public void execute_plan(string json_path) {
        // read the plan.json file
        auto json_data = cast(JSONValue) parseJSON(readText(json_path));
        size_t num_chunks = json_data.array.length;

        import std.range : iota;
        import std.parallelism : parallel;

        shared size_t completed_chunks = 0;
        foreach (i; parallel(iota(0, num_chunks))) {
            this.finish_chunk(json_path, i + 1);

            atomicOp!"+="(completed_chunks, 1);
            fileLogger.infof("Progress: %d/%d chunks completed.", completed_chunks, num_chunks);
        }
    }

    public void finish_chunk(string json_path, size_t chunk_id, int attempt = 0) {
        int MAX_ATTEMPTS = 3;
        ProcessResult res = this.run_chunk(json_path, chunk_id);

        if (res.success) {
            return;
        }

        if (res.exitCode == 1) {
            return;
        }

        // Error code 2 means process failed due to logical error, we need to retry
        if (res.exitCode == 2 && attempt < MAX_ATTEMPTS) {
            // Exponential backoff before retrying
            int backoffTime = cast(int) pow(1.42, attempt) * 1000; // in milliseconds
            fileLogger.warningf("Retrying Chunk %d after %d ms (Attempt %d/%d)",
                chunk_id, backoffTime, attempt + 1, MAX_ATTEMPTS);
            Thread.sleep(dur!"msecs"(backoffTime));
            return this.finish_chunk(json_path, chunk_id, attempt + 1);
        }

        fileLogger.errorf("Chunk %d failed after %d attempts. Skipping.", chunk_id, MAX_ATTEMPTS);

        return;
    }

    private ProcessResult run_chunk(string jsonPath, size_t chunk_id) {

        // spawnProcess connects to our stdout by default
        try {
            fileLogger.infof("Spawning process %s %s %s %s", pythonPath, scriptPath, jsonPath, chunk_id
                    .to!string);
            auto pid = spawnProcess([
                pythonPath, scriptPath, jsonPath, chunk_id.to!string
            ]);
            auto exitCode = wait(pid);

            bool ok = (exitCode == 0);
            if (ok) {
                fileLogger.infof("Chunk %d completed successfully.", chunk_id);
            } else {
                fileLogger.errorf("Chunk %d failed with exit code %d", chunk_id, exitCode);
            }

            return ProcessResult(chunk_id, ok, exitCode);
        }
        catch (Exception e) {
            fileLogger.errorf("System error spawning Chunk %d: %s", chunk_id, e.msg);
            return ProcessResult(chunk_id, false, -1);
        }
    }
}

public ProcessController get_runner(TaskMode mode, string pythonPath) {
    string baseDir = thisExePath().dirName();

    string scriptName;
    switch (mode) {
    case TaskMode.ALIGN:
        scriptName = "align_worker.py";
        break;
    case TaskMode.TEST:
        scriptName = "test_worker.py";
        break;
    case TaskMode.TILING:
        scriptName = "tiling_worker.py";
        break;
    case TaskMode.MOCK:
    default:
        scriptName = "mock_worker.py";
        break;
    }

    string scriptPath = buildPath(baseDir, "..", "engine", scriptName).absolutePath();

    enforce(scriptPath.exists,
        "CRITICAL ERROR: Python worker script not found at: " ~ scriptPath);

    fileLogger.infof("Runner initialized. Engine script resolved to: %s", scriptPath);

    return new ProcessController(pythonPath, scriptPath);
}
