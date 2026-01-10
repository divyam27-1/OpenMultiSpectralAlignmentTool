module process_control_h;

import std.string;
import std.process : Pid;
import std.datetime.systime : SysTime;

enum TaskMode
{
    MOCK,
    ALIGN,
    TEST,
    TILING
}

struct ProcessResult
{
    size_t chunk_id;
    bool success;
    int exitCode;
}

struct ProcessControlBlock
{
    Pid pid;
    size_t chunk_id;
    string json_path;
    SysTime start_time;
    int attempt;
}

__gshared TaskMode mode;