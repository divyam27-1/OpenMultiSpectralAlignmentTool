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

import worker_utils as wu
from services import alignment_testing as om_test
from services import alignment as om_align
from services import get_cam_mtx, get_dist_coeff

def main():    
    # PROCESS THE ARGV
    if len(sys.argv) < 4:
        logging.fatal(f"Invalid number of arguments provided: {len(sys.argv)} ({sys.argv}), expected at least 4.")
        sys.exit(1)

    workflow = sys.argv[1]
    plan_path = sys.argv[2]
    task_chunk = int(sys.argv[3])
    
    if task_chunk < 0:
        logging.fatal(f"Invalid task chunk number provided: {task_chunk}, expected a positive integer.")
        sys.exit(1)

    # PROCESS THE PLAN
    with open(plan_path) as f:
        plan = json.load(f)

    task = plan[task_chunk]
    
    task_logfile = task.get("logfile", None)
    if task_logfile is None:
        logging.warning(f"Task chunk {task_chunk} logfile not provided")
        task_logfile = os.path.join(os.getcwd(), "log", f"worker_{task_chunk}.txt")

    logging.basicConfig(
        filename=task_logfile,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    task_images_paths = task.get("images", None)
    if task_images_paths is None:
        logging.error(f"Task chunk {task_chunk} does not contain 'images'.")
        sys.exit(1)

    task_images_count = task.get("image_count", None)
    if task_images_count is None:
        logging.warning(f"Task chunk {task_chunk} does not contain 'image_count'.")
        task_images_count = len(task_images_paths)
    
    logging.info(f"Loaded plan for task chunk {task_chunk}, processing {task_images_count} images.")
    
    image_metadata = task.get("image_metadata", None)
    if image_metadata is None:
        logging.error(f"Task chunk {task_chunk} does not contain 'image_metadata'.")
        sys.exit(1)

    logging.info(f"Loaded image metadata for task chunk {task_chunk}")
    
    # LOAD IMAGES AND PERFORM THE TASK
    batches_idx = list(range(task_images_count))[0::wu.LOADING_BATCH_SIZE]
    
    if workflow == "MOCK":
        
        time.sleep(random.randint(1,3))
        
    elif workflow == "ALIGN":
        
        save_queue = Queue()
        results_dir = os.path.join(os.path.dirname(task_logfile), "..", "aligned")
        if not os.path.exists(results_dir):
            os.makedirs(results_dir, exist_ok=True)
        
        saver_thread = threading.Thread(
            target=wu.concurrent_saver_thread,
            args=(save_queue, results_dir),
            name=f"{task_chunk}_saver_thread",
            daemon=True
        )
        saver_thread.start()
        
        # Get camera matrix and distortion coefficients from image metadata
        for band, meta in image_metadata.items():
            meta["DistCoeff"] = get_dist_coeff(meta)
            meta["CameraMatrix"] = get_cam_mtx(meta)

        for idx in batches_idx:
            batch_paths = task_images_paths[idx : idx+wu.LOADING_BATCH_SIZE]
            batch = wu.load_chunk(batch_paths)

            # Align the Images Vertically
            aligned_samples = []
            for image in batch:
                aligned_samples.append(
                    om_align.align_multispectral_sample(image, image_metadata, base_band='NIR')
                )

            if not aligned_samples:
                logging.error(f"Alignment not done correctly for chunk {task_chunk} batch {idx}")
                sys.exit(1)
            
            for metadata, band_data in zip(batch_paths, aligned_samples):
                save_queue.put({
                    "bands": band_data,
                    "fname_base": metadata.get("base_name"),
                    "extension": metadata.get("extension", ".tif")
                })

            del aligned_samples
            del batch  
        
        logging.info(f"All images in chunk {task_chunk} sent to save queue. Cleaning up...")
        save_queue.put(None)
        saver_thread.join()
        
    elif workflow == "TEST":
        
        all_phase_results = []
        
        for idx in batches_idx:
            batch_paths = task_images_paths[idx : idx + wu.LOADING_BATCH_SIZE]
            batch_images = wu.load_chunk(batch_paths)
            batch_samples = []
            
            for metadata, band_data in zip(batch_paths, batch_images):
                batch_samples.append({
                    "fname_base": metadata.get("base_name"),
                    "class": metadata.get("class", "default"),
                    "bands": band_data
                })
                
            batch_phase_results = om_test.compute_phase_correlations(batch_samples)
            if batch_phase_results:
                all_phase_results.extend(batch_phase_results)
                
            del batch_images
            del batch_samples
            
            if (idx // wu.LOADING_BATCH_SIZE) % 10 == 0:
                gc.collect()
                
        # --- OUTSIDE THE LOOP: Final Global Analysis ---
        
        if not all_phase_results:
            logging.warning(f"No valid phase correlations found for chunk {task_chunk}")
            print(f"SUCCESS: Completed chunk {task_chunk} (No correlations found)")
            sys.exit(0)
            
        phase_results_filtered = om_test.filter_outliers(all_phase_results)
        phase_results_summary = om_test.summarize_shifts(phase_results_filtered)
        
        results_dir = os.path.join(os.path.dirname(task_logfile), "..", "results")
        os.makedirs(results_dir, exist_ok=True)
        results_fpath = os.path.abspath(os.path.join(results_dir, f"results_{task_chunk}.csv"))
        
        om_test.write_phase_csv(phase_results_filtered, results_fpath)
        logging.info(f"Written output of chunk {task_chunk} to {results_fpath}")
        
        summary_str = ', '.join(f'{k}: {v:.4f}' if isinstance(v, float) else f'{k}: {v}' 
                               for k, v in phase_results_summary.items())
        print(f"\nSummary of Chunk {task_chunk}:\n{summary_str}")
        
        del all_phase_results

    elif workflow == "TILE":
        pass
        
    logging.info(f"SUCCESS: Completed processing chunk {task_chunk}")
    print(f"SUCCESS: Completed processing chunk {task_chunk}")
    

if __name__ == "__main__":
    main()