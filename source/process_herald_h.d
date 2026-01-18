module process_herald_h;

import zmqd;

import std.datetime : Duration;
import std.datetime.stopwatch : StopWatch, AutoStart;
import std.container.dlist;
import loggers : heraldLogger;

// Encapsulates a single pairing between server and worker
class WorkerConnection
{
    ulong pid;
    Socket* socketIn;
    Socket* socketOut;
    const Duration hbInterval;
    int watchdogTimeouts = 0;
    StopWatch sw;
    DList!HeraldMessage mailbox;

    this(ulong pid, Socket* si, Socket* so, const Duration inv)
    {
        this.pid = pid;
        this.socketIn = si;
        this.socketOut = so;
        this.hbInterval = inv;
        this.sw = StopWatch(AutoStart.yes);
    }

    void resetWatchdog()
    {
        this.watchdogTimeouts = 0;
        this.sw.reset();
    }

    void incrementWatchdog()
    {
        this.watchdogTimeouts += 1;
    }

    void recvAllMessages()
    {
        import std.array : array;
        import std.conv : to;

        ubyte[1024] buf;
        bool hasMessage = true;
        while (hasMessage)
        {
            try
            {
                auto result = this.socketIn.tryReceive(buf);
                if (result[1])
                {
                    string msgStr = cast(string) buf[0 .. result[0]];
                    HeraldMessage msg = msgStr.decodeFromString;
                    this.resetWatchdog();

                    switch (msg.msgType)
                    {
                    case WorkerMessages.Heartbeat:
                        continue;
                    case WorkerMessages.MessageInvalid:
                        heraldLogger.warn("Received invalid message from worker %s", this.pid);
                        continue;
                    default:
                        break;
                    }

                    this.mailbox.insertBack(msg);
                }
                else
                {
                    hasMessage = false;
                }
            }
            catch (Exception e)
            {
                heraldLogger.error("Failed to receive message from worker %s: %s", this.pid, e.msg);
                hasMessage = false;
            }
        }
    }
}

struct HeraldMessage
{
    ulong workerId;
    WorkerMessages msgType;
    uint[] payload;

    // To instantiate basic message types like HB etc where rest dont matter
    this(WorkerMessages msg)
    {
        this.workerId = 0;
        this.msgType = msg;
        this.payload = [];
    }

    string encodeToString()
    {
        import std.json;

        JSONValue obj = JSONValue([
            "workerId": JSONValue(this.workerId),
            "msgType": JSONValue(cast(int) this.msgType),
            "payload": JSONValue(this.payload)
        ]);

        return obj.toString();
    }
}

HeraldMessage decodeFromString(string jsonStr)
{
    JSONValue val = parseJSON(jsonStr);
    HeraldMessage msg;
    uint completion = 0;

    if ("workerId" in val)
    {
        msg.workerId = val["workerId"].get!ulong;
        completion |= 0b1;
    }

    if ("msgType" in val)
    {
        msg.msgType = cast(WorkerMessages) cast(int) val["msgType"].get!long;
        completion |= 0b10;
    }

    if ("payload" in val && val["payload"].type == JSONType.array)
    {
        foreach (item; val["payload"].array)
        {
            msg.payload ~= cast(uint) item.get!long;
        }
        completion |= 0b100;
    }

    if (!(completion && 0b001))
        heraldLogger.warn(
            "Failed to fully decode HeraldMessage from JSON: missing field 'workerId'");
    if (!(completion && 0b010))
        heraldLogger.warn(
            "Failed to fully decode HeraldMessage from JSON: missing field 'msgType'");
    if (!(completion && 0b100))
        heraldLogger.warn(
            "Failed to fully decode HeraldMessage from JSON: missing field 'payload'");

    return completion && 0b11 ? msg : HeraldMessage(WorkerMessages.MessageInvalid);
}

// If a client receives a messsage of this type, it should reason it as a command
// If server receives a message of this type, it should reason it as a completion of said command
enum WorkerMessages : int
{
    Heartbeat = 0,
    TaskStart = 1,
    TaskFinish = 2,
    WorkerFlush = 3,
    WorkerStop = 4,
    WorkerError = 5,
    MessageInvalid = 400,
}

enum TaskFinishCodes : int
{
    Success = 0,
    Failed = 1,
    RetryRequested = 2,
}

enum WorkerErrorCodes : int
{
    UnknownError = 0,
    InitializationFailed = 1,
    RuntimeFailure = 2,
    WatchdogTimeout = 3,
}
