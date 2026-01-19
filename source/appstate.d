module appstate;

import std.path;

__gshared string _targetPath;

@property string targetPath()
{
    return _targetPath;
}

void setTargetPath(string path)
{
    assert(_targetPath.length == 0, "targetPath already set");
    _targetPath = path;
}

enum TaskMode : int
{
    MOCK = 0,
    ALIGN = 1,
    TEST = 2,
    TILING = 3
}

__gshared TaskMode mode;
