module loggers;

import std.logger;

__gshared FileLogger mainLogger;
__gshared FileLogger planLogger;
__gshared FileLogger processLogger;
__gshared string workerLogPath;