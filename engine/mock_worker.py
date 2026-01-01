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
    if task_chunk < 0:
        print(f"Invalid task chunk number provided: {task_chunk}, expected a positive integer.")
        sys.exit(1)

    with open(plan_path) as f:
        plan = json.load(f)
    
    task = plan[task_chunk]
    task_images_paths = task.get("images", None)
    if task_images_paths is None:
        print(f"Task chunk {task_chunk} does not contain 'images'.")
        sys.exit(1)
    
    print(f"[Python] Loaded plan for task chunk {task_chunk}, processing {len(task_images_paths)} images.")
    
    for image in task_images_paths:
        bands_opened, bands_data = [], []
        image_base_name = image.get("base_name", None)
        if image_base_name is None:
            print(f"Task chunk {task_chunk} does not contain 'base_name' in 'images'.")
            sys.exit(1)
        try:
            print(f"[Python] Starting processing task chunk {task_chunk} with base image name: {image_base_name}")
            band_obj = image.get("bands", None)
            if band_obj is None:
                print(f"[Python] FATAL: No 'bands' found for image {image_base_name}")
                sys.exit(1)
            
            for band_name, band_path in band_obj.items():
                try:
                    bands_opened.append(band_name)
                    with open(band_path, 'rb') as band_file:
                        band_data = band_file.read()
                        bands_data.append(band_data)
    
                except Exception as e:
                    print(f"[Python] FATAL: Error opening band {band_name} for {image_base_name}: {e}")
                    sys.exit(1)
                    
            print(f"[Python] Data processed for file: {image_base_name}, Data: {list(zip(bands_opened, bands_data))}")

        except Exception as e:
            print(f"[Python] FATAL: Error opening image bands for {image_base_name}: {e}")
            sys.exit(1)
    
    time.sleep(random.uniform(1.0, 3.0))
    
    # simulate random chance of failure
    if random.random() < 0.30:
        print(f"[Python] FAILURE: Data Corruption on chunk {task_chunk}")
        sys.exit(2) # error code 2 means process should be retried due to some logical error
    
    print(f"[Python] SUCCESS: Completed processing chunk {task_chunk}")
    
if __name__ == "__main__":
    main()