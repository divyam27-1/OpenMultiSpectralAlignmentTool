import sys
import os
import gc
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import json
import time
import random
import logging
from queue import Queue
import threading

import protocol
import worker_utils as wu
from services import alignment_testing as om_test
from services import alignment as om_align
from services import get_cam_mtx, get_dist_coeff

import zmq

if __name__ == "__main__":
    selfpid = os.getpid()
    plan_path = os.environ.get("OMSPEC_PLANPATH", None)
    input_stream = os.environ.get("OMSPEC_OUTPUT_STREAM", None)
    output_stream = os.environ.get("OMSPEC_INPUT_STREAM", None)
    logdir = os.environ.get("OMSPEC_LOGDIR", None)
    
    if input_stream is None or output_stream is None:
        logging.fatal("Input or output stream environment variables not set.")
        sys.exit(1)
    
    if plan_path is None:
        logging.fatal("Plan Path Not Provided")
        sys.exit(1)
    
    if logdir is None:
        logging.warning("Log directory environment variable not set, using default './log'.")
        logdir = os.path.join(os.getcwd(), "log")
    
    logging.basicConfig(
        filename=os.path.join(logdir, f"worker_{selfpid}.log"),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    plan = json.load(open(plan_path))
    num_chunks = len(plan)
    logging.info(f"Worker {selfpid} started with plan {plan_path}.")
    
    context = zmq.Context()
    
    socket_in = context.socket(zmq.PULL)
    socket_in.connect(input_stream)
    socket_in.setsockopt(zmq.RCVTIMEO, 1000)
    socket_in.setsockopt(zmq.LINGER, 0)
    
    socket_out = context.socket(zmq.PUSH)
    socket_out.connect(output_stream)
    
    running = True
    
    while running:
        try:
            message = socket_in.recv_string()
            command = json.loads(message)
            
            address = command.get("workerId", None)
            if not protocol.match_address(str(address), str(selfpid)):      # this function has internal logic that calibrates for address 0 being a broadcast address
                logging.error(f"Received task for workerId {address}, but my workerId is {selfpid}. Ignoring task.")
                socket_out.send_string(json.dumps(
                    {
                        "workerId": selfpid,
                        "msgType": protocol.WorkerMessages.WorkerError,
                        "payload": [protocol.WorkerErrorCodes.InitializationFailed]
                    }
                ))
                continue
            
            task = command.get("msgType", None)
            if task not in list(protocol.WorkerMessages):
                logging.error(f"Received invalid task type: {task}.")
                socket_out.send_string(json.dumps(
                    {
                        "workerId": selfpid,
                        "msgType": protocol.WorkerMessages.MessageInvalid,
                        "payload": []
                    }
                ))
                continue
            
            # --- HEARTBEAT --- #            
            if task == protocol.WorkerMessages.Heartbeat:
                logging.debug(f"HB_{selfpid}")
                socket_out.send_string(json.dumps(
                    {
                        "workerId": selfpid,
                        "msgType": protocol.WorkerMessages.Heartbeat,
                        "payload": []
                    }
                ))
                continue
            
            # --- TASK START --- #            
            if task == protocol.WorkerMessages.TaskStart:
                # Parse Payload
                payload = command.get("payload", None)
                if payload is None:
                    logging.error(f"TaskStart message missing payload.")
                    socket_out.send_string(json.dumps(
                        {
                            "workerId": selfpid,
                            "msgType": protocol.WorkerMessages.WorkerError,
                            "payload": [protocol.WorkerErrorCodes.RuntimeFailure]
                        }
                    ))
                    continue
                
                if len(payload) < 3:
                    logging.error(f"TaskStart payload incomplete: {payload}.")
                    socket_out.send_string(json.dumps(
                        {
                            "workerId": selfpid,
                            "msgType": protocol.WorkerMessages.WorkerError,
                            "payload": [protocol.WorkerErrorCodes.RuntimeFailure]
                        }
                    ))
                    continue
                
                # Parse Workflow
                workflow_type = payload[0]
                if workflow_type not in list(protocol.TaskWorkflows):
                    logging.error(f"Received unknown workflow type: {workflow_type}.")
                    socket_out.send_string(json.dumps(
                        {
                            "workerId": selfpid,
                            "msgType": protocol.WorkerMessages.WorkerError,
                            "payload": [protocol.WorkerErrorCodes.RuntimeFailure]
                        }
                    ))
                    continue
                
                # Parse Task Chunk
                task_chunk_id = payload[1]
                if task_chunk_id >= num_chunks:
                    logging.error(f"Task chunk {task_chunk_id} not found in plan.")
                    socket_out.send_string(json.dumps(
                        {
                            "workerId": selfpid,
                            "msgType": protocol.WorkerMessages.WorkerError,
                            "payload": [protocol.WorkerErrorCodes.RuntimeFailure]
                        }
                    ))
                    continue
                task_chunk = plan[task_chunk_id]
                
                # Parse Task Image
                task_image_id = payload[2]
                chunk_images = task_chunk.get("images", None)
                if chunk_images is None or task_image_id >= len(chunk_images):
                    logging.error(f"Task item {task_image_id} not found in chunk {task_chunk_id}.")
                    socket_out.send_string(json.dumps(
                        {
                            "workerId": selfpid,
                            "msgType": protocol.WorkerMessages.WorkerError,
                            "payload": [protocol.WorkerErrorCodes.RuntimeFailure]
                        }
                    ))
                    continue
                
                # Finally perform task
                task_image = chunk_images[task_image_id]
                logging.info(f"Worker {selfpid} starting workflow {workflow_type} on task chunk {task_chunk_id}, image {task_image_id}.")
                
                # Send ACK that task has started
                socket_out.send_string(json.dumps(
                    {
                        "workerId": selfpid,
                        "msgType": protocol.WorkerMessages.TaskStart,
                        "payload": []
                    }
                ))
                
                status_code = None
                if workflow_type == protocol.TaskWorkflows.Mock:
                    time.sleep(random.randint(1,3)) # Only Mock workflow supported for now
                    r = random.random()
                    if r < 0.1:
                        status_code = protocol.TaskFinishCodes.Failed
                    elif r < 0.3:
                        status_code = protocol.TaskFinishCodes.RetryRequested
                    else:
                        status_code = protocol.TaskFinishCodes.Success
                # TODO: Implement other workflows here by function call to new orchestrators library
                
                # Send Task Finish
                logging.info(f"Worker {selfpid} finished workflow {workflow_type} on task chunk {task_chunk_id}, image {task_image_id}.")
                socket_out.send_string(json.dumps(
                    {
                        "workerId": selfpid,
                        "msgType": protocol.WorkerMessages.TaskFinish,
                        "payload": [status_code]
                    }
                ))
                
            # --- WORKER STOP --- #
            if task == protocol.WorkerMessages.WorkerStop:
                logging.info(f"Worker {selfpid} received stop command. Exiting.")
                running = False
                continue
        
        except zmq.Again:
            time.sleep(0.1)
            continue 
    
    socket_out.send_string(json.dumps(
        {
            "workerId": selfpid,
            "msgType": protocol.WorkerMessages.WorkerStop,
            "payload": []
        }
    ))
    
    socket_in.close()
    socket_out.close()
    context.term()
    logging.info(f"Worker {selfpid} has exited.")