module loggers;

import std.logger;

__gshared FileLogger mainLogger;
__gshared FileLogger planLogger;
__gshared FileLogger processLogger;
__gshared FileLogger monitorLogger;
__gshared FileLogger managerLogger;
__gshared string workerLogPath;