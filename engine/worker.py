import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import json
import time
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import worker_utils as wu
from services import alignment_testing as om_test
from services import alignment as om_align
from services import get_cam_mtx, get_dist_coeff

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
    
    image_metadata = task.get("image_metadata", None)
    if image_metadata is None:
        logging.error(f"Task chunk {task_chunk} does not contain 'image_metadata'.")
        sys.exit(1)
    
    logging.info(f"Loaded image metadata for task chunk {task_chunk}")
    
    images = wu.load_chunk(task_images_paths)
    
    if workflow == "MOCK":
        time.sleep(random.randint(1,3))
        
    elif workflow == "ALIGN":
        
        # Get camera matrix and distortion coefficients from image metadata
        for band, meta in image_metadata.items():
            meta["CameraMatrix"] = get_cam_mtx(meta)
            meta["DistCoeff"] = get_dist_coeff(meta)
        
        # Align the Images Vertically
        aligned_samples = []
        for image in images:
            aligned_samples.append(
                om_align.align_multispectral_sample(image, image_metadata, base_band='NIR')
            )

        if not aligned_samples:
            logging.error(f"Alignment not done correctly for chunk {task_chunk}")
            sys.exit(1)

        logging.info(f"Alignment step finished for chunk {task_chunk}")
        
        # Save Results Paralelly
        results_dir = os.path.join(os.path.dirname(task_logfile), "..", "aligned")
        if not os.path.exists(results_dir):
            os.makedirs(results_dir, exist_ok=True)

        with ThreadPoolExecutor(max_workers=16) as ex:
            save_statuses = []
            futures = [
                ex.submit(wu.save_multispectral_image,
                          aligned_sample,
                          "ALIGNED_"+chunk_metadata.get("base_name"),
                          results_dir,
                          chunk_metadata.get("extension", ".tif"))
                for chunk_metadata, aligned_sample
                in zip(task_images_paths, aligned_samples)
            ]

            for future in as_completed(futures):
                save_statuses.append(str(future.result()))

            logging.info(save_statuses)
            
        del aligned_samples              

    elif workflow == "TEST":
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
        results_dir = os.path.join(os.path.dirname(task_logfile), "..", "results")
        if not os.path.exists(results_dir):
            os.makedirs(results_dir, exist_ok=True)
        
        results_fpath = os.path.abspath(os.path.join(results_dir, f"results_{task_chunk}.csv"))
        om_test.write_phase_csv(phase_results_filtered, results_fpath)
        logging.info(f"Written output of chunk {task_chunk} to {results_fpath}")
        print(f"\nSummary of Chunk {task_chunk}:\n{', '.join(f'{k}: {v:.4f}' if isinstance(v, float) else f'{k}: {v}' for k, v in phase_results_summary.items())}")

        del phase_results
    
    elif workflow == "TILE":
        pass
        
    logging.info(f"SUCCESS: Completed processing chunk {task_chunk}")
    print(f"SUCCESS: Completed processing chunk {task_chunk}")
    
    del images

if __name__ == "__main__":
    main()