module process_herald;

import zmqd;

import std.algorithm;
import std.array;
import std.datetime : Duration;
import core.sync.mutex : Mutex;
import std.datetime.stopwatch;
import std.concurrency;
import core.thread;

import process_herald_h;
import loggers : heraldLogger;
import appstate : mode;

class ProcessHerald
{
    public WorkerConnection[uint] workers;
    private HeraldMessage[] globalInbox;

    public Mutex workersMutex;
    private Mutex inboxMutex;
    private Tid heraldTid;

    private const Duration hbInterval = 3.seconds;
    private const Duration hbTimeout = 10.seconds;

    public static string hbMsg;

    static this()
    {
        hbMsg = HeraldMessage(WorkerMessages.Heartbeat).encodeToString;
    }

    this()
    {
        this.inboxMutex = new Mutex();
        this.workersMutex = new Mutex();
        this.heraldTid = spawn(&heraldWorker, cast(shared) this, thisTid);
    }

    void reserveEndpoints(uint tempId)
    {
        send(this.heraldTid, M_ReserveEndpoints(tempId));
    }

    void unreserveEndpoints(ZMQEndpoints endpoints)
    {
        send(this.heraldTid, M_UnreserveEndpoints(endpoints));
    }

    void registerWorker(uint workerId, ZMQEndpoints endpoints)
    {
        send(this.heraldTid, M_RegisterRequest(workerId, cast(immutable) endpoints));
    }

    void deregisterWorker(uint workerId)
    {
        send(this.heraldTid, M_DeregisterRequest(workerId));
    }

    bool sendTask(uint workerId, uint chunkId, uint imageIdx)
    {
        // Check if worker exists locally first (optional optimization)
        synchronized (workersMutex)
        {
            if ((workerId in this.workers) == null)
                return false;
        }

        send(this.heraldTid, M_SendTaskRequest(workerId, chunkId, imageIdx));
        return true;
    }

    HeraldMessage[] popResponses()
    {
        // Flush all mailboxes into the globalInbox
        this.collapseMailboxes();

        synchronized (inboxMutex)
        {
            auto responses = globalInbox.dup;
            globalInbox.length = 0;
            return responses;
        }
    }

    private void collapseMailboxes()
    {
        HeraldMessage[] batch;

        WorkerConnection[] workersCopy;
        synchronized (workersMutex)
        {
            workersCopy = workers.values.array;
        }

        foreach (WorkerConnection conn; workersCopy)
        {
            while (!conn.mailbox.empty)
            {
                batch ~= conn.mailbox.front();
                conn.mailbox.removeFront();
            }
        }

        synchronized (inboxMutex)
        {
            this.globalInbox ~= batch;
        }
    }

    void pushPriorityInbox(HeraldMessage msg)
    {
        synchronized (inboxMutex)
        {
            this.globalInbox ~= msg;
        }
    }

    Duration getHbInterval()
    {
        return this.hbInterval;
    }
}

void heraldWorker(shared ProcessHerald sharedHerald, Tid parentTid)
{
    bool spin = true;
    ProcessHerald herald = cast(ProcessHerald) sharedHerald;
    StopWatch sw = StopWatch(AutoStart.yes);

    while (spin)
    {
        // --- 1. HANDLE INCOMING COMMANDS (Registration / Stop) ---
        receiveTimeout(15.msecs,
            (M_ReserveEndpoints req) {

            Socket* socketOut = new Socket(SocketType.push);
            socketOut.bind("tcp://127.0.0.1:*");
            string outEP = cast(string) socketOut.lastEndpoint;

            Socket* socketIn = new Socket(SocketType.pull);
            socketIn.bind("tcp://127.0.0.1:*");
            string inEP = cast(string) socketIn.lastEndpoint;

            ZMQEndpoints endpoints = ZMQEndpoints(inEP, outEP, socketIn, socketOut);

            send(parentTid, cast(immutable) endpoints);
        },
            (M_UnreserveEndpoints req) {

            ZMQEndpoints ep = cast(ZMQEndpoints) req.endpoints;
            ep.socketIn.close();
            ep.socketOut.close();

            send(parentTid, M_RegisterResponse(true));
        },
            (M_RegisterRequest req) {

            synchronized (herald.workersMutex)
            {
                auto conn = new WorkerConnection(
                    req.workerId,
                    cast(Socket*) req.endpoints.socketIn, // Pointer to the PULL socket
                    cast(Socket*) req.endpoints.socketOut, // Pointer to the PUSH socket
                    herald.getHbInterval()
                );

                herald.workers[req.workerId] = conn;
            }

            heraldLogger.infof("Herald Thread: Successfully assumed ownership of sockets for Worker %d",
                req.workerId);
            send(parentTid, M_RegisterResponse(true));
        },
            (M_DeregisterRequest req) {

            synchronized (herald.workersMutex)
            {
                auto pConn = req.workerId in herald.workers;
                if (pConn)
                {
                    // Close sockets in the same thread that used them
                    (*pConn).socketIn.close();
                    (*pConn).socketOut.close();
                    herald.workers.remove(req.workerId);
                }
            }

            heraldLogger.infof("Herald Thread: Deregistered Worker %d", req.workerId);
        },
            (M_SendTaskRequest req) {

            synchronized (herald.workersMutex)
            {
                auto pConn = req.workerId in herald.workers;
                if (pConn)
                {
                    HeraldMessage msg = HeraldMessage();
                    msg.workerId = req.workerId;
                    msg.msgType = WorkerMessages.TaskStart;
                    msg.payload = [
                        cast(int) mode,
                        cast(int) req.chunkId,
                        cast(int) req.imageIdx
                    ];

                    string wireData = msg.encodeToString();

                    bool ret = pConn.socketOut.trySend(wireData);
                    if (!ret)
                        heraldLogger.errorf("ZMQ Send Failed for Worker %d", req.workerId);
                }
            }
        },
            (string msg) {

            if (msg == "STOP")
                spin = false;
        }
        );

        if (!spin)
            break;

        // Get a local snapshot of workers to minimize lock contention
        WorkerConnection[] workers;
        synchronized (herald.workersMutex)
        {
            workers = herald.workers.values.array;
        }

        // --- 2. SECTION A: SEND HEARTBEATS ---
        foreach (conn; workers)
        {
            if (conn.sw.peek >= herald.getHbInterval())
            {
                try
                {
                    conn.socketOut.send(ProcessHerald.hbMsg);
                    conn.incrementWatchdog();
                }
                catch (Exception e)
                {
                    heraldLogger.errorf("Heartbeat Fail (Worker %d): %s", conn.pid, e.msg);
                }
                conn.sw.reset();
            }
        }

        // --- 3. SECTION B: RECEIVE MESSAGES & WATCHDOGS ---
        foreach (conn; workers)
        {
            conn.recvAllMessages(); // This should populate conn.mailbox

            if (conn.watchdogTimeouts >= 7)
            {
                heraldLogger.criticalf("Worker %d Watchdog Timeout!", conn.pid);

                HeraldMessage timeoutErr = HeraldMessage(
                    conn.pid,
                    WorkerMessages.WorkerError,
                    [cast(uint) WorkerErrorCodes.WatchdogTimeout]
                );

                herald.pushPriorityInbox(timeoutErr);
                conn.resetWatchdog();
            }
        }
    }

    heraldLogger.infof("Herald Worker Thread exiting. Ran for %d secs", sw.peek().total!"seconds");
}
