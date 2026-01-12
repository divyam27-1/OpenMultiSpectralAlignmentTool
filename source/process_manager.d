module process_manager;

import std.stdio;
import std.parallelism : totalCPUs;
import std.process : tryWait;
import std.process : Pid;

import process_manager_h;

class ProcessManager {
    private ProcessControlBlock[Pid] pcbMap;
    public immutable size_t physicalCoreCount;

    this() {
        uint systemCoreCount = getPhysicalCoreCount();
        this.physicalCoreCount = systemCoreCount == 1 ? totalCPUs / 2 : systemCoreCount;        // TODO: add a user cfg option to set SMT ratio
    }

    // Checks OS for finished processes and returns their results
    ProcessResult[] collectResults() {
        ProcessResult[] results;
        foreach (pid, pcb; pcbMap) {
            auto res = tryWait(pid);
            if (res.terminated) {
                results ~= ProcessResult(pcb.chunk_id, res.status);
                pcbMap.remove(pid);
            }
        }

        return results;
    }
}