module process_manager;

import std.stdio;
import std.parallelism : totalCPUs;

import process_manager_h;

class ProcessManager {
    private ProcessControlBlock[Pid] pcbMap;
    public immutable size_t physicalCoreCount;

    this() {
        this.physicalCoreCount = getPhysicalCoreCount();
        if (this.physicalCoreCount == 1) this.physicalCoreCount = totalCPUs / 2;        // TODO: add a user cfg option to set SMT ratio
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

    // New logic: Check hardware RAM and CPU availability
    bool canSpawn(size_t requiredMB) {
        auto sys = getResources(); // From your process_monitor_h
        bool hasCpuSlot = pcbMap.length < physicalCoreCount;
        bool hasRamSlot = (sys.availableRAM_MB > 1024) && (requiredMB < sys.availableRAM_MB); 
        return hasCpuSlot && hasRamSlot;
    }
}

void main() {
    uint pCores = getPhysicalCoreCount();
    writeln("--- CPU Topology ---");
    writeln("Physical Cores: ", pCores);
    writeln("Logical Cores:  ", totalCPUs);
    
    if (pCores == totalCPUs) {
        writeln("Note: Hyper-threading is likely disabled or not supported.");
    } else {
        writeln("Note: Hyper-threading detected.");
    }
}

