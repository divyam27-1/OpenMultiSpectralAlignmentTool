import sys
import json
import time
import random

def main():    
    if len(sys.argv) < 3:
        print(f"Invalid number of arguments provided: {len(sys.argv)} ({sys.argv}), expected at least 3.")
        sys.exit(1)

    plan_path = sys.argv[1]
    task_chunk = int(sys.argv[2])
    if task_chunk < 1:
        print(f"Invalid task chunk number provided: {task_chunk}, expected a positive integer.")
        sys.exit(1)

    with open(plan_path) as f:
        plan = json.load(f)
    
    task = plan[task_chunk - 1]
    task_images_paths = task.get("images", None)
    if task_images_paths is None:
        print(f"Task chunk {task_chunk} does not contain 'images'.")
        sys.exit(1)
    
    bands_opened, bands_data = [], []
    image_base_name = task_images_paths.get("base_name", None)
    if image_base_name is None:
        print(f"Task chunk {task_chunk} does not contain 'base_name' in 'images'.")
        sys.exit(1)
    try:
        print(f"[Python] Starting processing task chunk {task_chunk} with base image name: {image_base_name}")
        for band in task_images_paths.get("bands", []):
            band_path = f"{image_base_name}_{band}.tif"

            with open(band_path) as f:
                band_data = f.read()
                bands_opened.append(band_path)
                bands_data.append(band_data)
    
    except Exception as e:
        print(f"[Python] FATAL: Error opening image bands for {image_base_name}: {e}")
        sys.exit(1) # error code 1 means fatal error, do not retry
    
    time.sleep(random.uniform(1.0, 3.0))
    
    # simulate random chance of failure
    if random.random() < 0.30:
        print("[Python] FAILURE: Simulated sensor data corruption!")
        sys.exit(2) # error code 2 means process should be retried due to some logical error
    
    print(f"[Python] Data processed for file: {image_base_name}, Data: {list(zip(bands_opened, bands_data))}")

if __name__ == "__main__":
    main()