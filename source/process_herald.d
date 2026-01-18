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

class ProcessHerald
{
    private WorkerConnection[uint] workers;
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

    string[2] registerWorker(uint workerId)
    {
        Socket* socketOut = new Socket(SocketType.push);
        socketOut.bind("tcp://127.0.0.1:*");
        string outEndpoint = cast(string) socketOut.lastEndpoint;

        Socket* socketIn = new Socket(SocketType.pull);
        socketIn.bind("tcp://127.0.0.1:*");
        string inEndpoint = cast(string) socketIn.lastEndpoint;

        synchronized (workersMutex)
        {
            workers[workerId] = new WorkerConnection(workerId, socketIn, socketOut, this.hbInterval);
        }
        heraldLogger.info("Registered worker %s with endpoints IN: %s OUT: %s", workerId, inEndpoint, outEndpoint);

        return [inEndpoint, outEndpoint];
    }

    void deregisterWorker(uint workerId)
    {
        synchronized (workersMutex)
        {
            if (workerId in this.workers)
            {
                auto conn = workers[workerId];
                conn.socketIn.close();
                conn.socketOut.close();
                workers.remove(workerId);
            }
        }

        heraldLogger.info("Deregistered worker %s", workerId);
    }

    HeraldMessage[] popMessages()
    {
        synchronized (inboxMutex)
        {
            auto messages = globalInbox.dup;
            globalInbox.length = 0;
            return messages;
        }
    }

    private void collapseMailboxes()
    {
        HeraldMessage[] batch;

        WorkerConnection[] workersCopy;
        synchronized (workersMutex) {
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

    void incrementWatchdog(uint workerId)
    {
        if (workerId in workers)
        {
            workers[workerId].watchdogTimeouts += 1;
        }
    }

    void resetWatchdog(uint workerId)
    {
        if (workerId in workers)
        {
            workers[workerId].watchdogTimeouts = 0;
        }
    }

    auto getWorkers()
    {
        return workers;
    }
}

void heraldWorker(shared ProcessHerald sharedHerald, Tid parentTid)
{
    bool spin = true;
    ProcessHerald herald = cast(ProcessHerald) sharedHerald;

    StopWatch sw = StopWatch(AutoStart.yes);

    while (spin)
    {
        WorkerConnection[] workers;
        synchronized (herald.workersMutex)
        {
            workers = herald.getWorkers().values.array;
        }

        // --- SECTION A: SEND HEARTBEATS ---
        foreach (conn; workers)
        {
            if (conn.sw.peek >= herald.hbInterval)
            {
                try
                {
                    conn.socketOut.send(ProcessHerald.hbMsg);
                    conn.incrementWatchdog();
                }
                catch (Exception e)
                {
                    heraldLogger.error("Failed to send heartbeat to worker %s: %s", conn.pid, e.msg);
                }
                conn.sw.reset();
            }
        }

        // --- SECTION B: RECEIVE MESSAGES AND RAISE WATCHDOG TIMEOUTS ---
        foreach (conn; workers)
        {
            conn.recvAllMessages();

            if (conn.watchdogTimeouts >= 3)
            {
                heraldLogger.critical("Worker %s timed out. Raising High-Priority Error.", conn.pid);

                HeraldMessage timeoutErr = HeraldMessage(
                    conn.pid,
                    WorkerMessages.WorkerError,
                    [WorkerErrorCodes.WatchdogTimeout]
                );

                // --- HIGH PRIORITY BYPASS ---
                // Instead of conn.mailbox, we go straight to the global inbox
                herald.pushPriorityInbox(timeoutErr);
                conn.resetWatchdog();
            }
        }

        if (sw.peek >= 400.msecs)
        {
            herald.collapseMailboxes();
            sw.reset();
        }

        receiveTimeout(10.msecs,
            (string msg) {
            if (msg == "STOP")
                spin = false;
        });
    }
}
