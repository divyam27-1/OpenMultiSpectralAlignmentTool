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
