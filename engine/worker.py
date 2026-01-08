import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import json
import time
import random
import logging
import worker_utils as wu
from services import alignment_testing as om_test

def main():    
    if len(sys.argv) < 4:
        logging.fatal(f"Invalid number of arguments provided: {len(sys.argv)} ({sys.argv}), expected at least 4.")
        sys.exit(1)

    workflow = sys.argv[1]
    plan_path = sys.argv[2]
    task_chunk = int(sys.argv[3])
    
    if task_chunk < 0:
        logging.fatal(f"Invalid task chunk number provided: {task_chunk}, expected a positive integer.")
        sys.exit(1)

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

    logging.info(f"Loaded plan for task chunk {task_chunk}, processing {len(task_images_paths)} images.")

    images = wu.load_chunk(task_images_paths)

    if workflow == "TEST":
            
        samples = []
        for metadata, band_data in zip(task_images_paths, images):
            samples.append({
                "fname_base": metadata.get("base_name"),
                "class": metadata.get("class", "default"), # Fallback if class isn't in JSON
                "bands": band_data
            })

        # 3. Call the service
        phase_results = om_test.compute_phase_correlations(samples)
        if not phase_results:
            logging.warning(f"No valid phase correlations found for chunk {task_chunk}")
            print(f"SUCCESS: Completed chunk {task_chunk} (No correlations found)")
            sys.exit(0)
        
        del samples

        phase_results_filtered = om_test.filter_outliers(phase_results)
        phase_results_summary = om_test.summarize_shifts(phase_results_filtered)
        
        # 4. Write the Summary to CSV in LOG (in future to write it to target Dir)
        target_dir = os.path.join(os.path.dirname(task_logfile), "..")
        results_fpath = os.path.abspath(os.path.join(target_dir, f"results_{task_chunk}.csv"))
        om_test.write_phase_csv(phase_results_filtered, results_fpath)
        logging.info(f"Written output of chunk {task_chunk} to {results_fpath}")
        print(f"\nSummary of Chunk {task_chunk}:\n{', '.join(f'{k}: {v:.4f}' if isinstance(v, float) else f'{k}: {v}' for k, v in phase_results_summary.items())}")

        del phase_results
        
    logging.info(f"SUCCESS: Completed processing chunk {task_chunk}")
    print(f"SUCCESS: Completed processing chunk {task_chunk}")
    
    del images

if __name__ == "__main__":
    main()