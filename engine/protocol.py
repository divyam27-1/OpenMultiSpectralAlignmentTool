from enum import IntEnum

class WorkerMessages(IntEnum):
    Heartbeat = 0
    TaskStart = 1
    TaskFinish = 2
    WorkerFlush = 3
    WorkerStop = 4
    WorkerError = 5
    MessageInvalid = 400
    
class TaskWorkflows(IntEnum):
    Mock = 0
    Align = 1
    Test = 2
    Tiling = 3
    
class TaskFinishCodes(IntEnum):
    Success = 0
    Failed = 1
    RetryRequested = 2

class WorkerErrorCodes(IntEnum):
    UnknownError = 0
    InitializationFailed = 1
    RuntimeFailure = 2
    WatchdogTimeout = 3
    UnknownWorkflow = 4

def match_address(address: str, self_address: str) -> bool:
    if address is None:
        return False
    
    return address == self_address or address == str(0)