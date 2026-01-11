import cv2
import numpy as np
import logging
import sys
import os
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed

LOADING_BATCH_SIZE = 1
SAVING_BATCH_SIZE = 6

def load_chunk(image_entries: list[dict[str, str]]) -> list[dict[str, np.ndarray]]:
    """
    Loads a chunk of image entries.

    Returns:
        List of images in the format:
        {
            "band1" : np.ndarray,
            "band2" : np.ndarray,
            ...
        }

    Exit semantics:
        Propagates exit(1) and exit(2) from load_image_bands unchanged.
    """
    if not isinstance(image_entries, list):
        logging.fatal("Image chunk is not a list.")
        sys.exit(1)

    loaded_chunk: list[dict[str, np.ndarray]] = []

    logging.info(f"Loading image chunk with {len(image_entries)} entries")

    for idx, image_entry in enumerate(image_entries):
        if not isinstance(image_entry, dict):
            logging.error(f"Invalid image entry at index {idx}: not a dict")
            sys.exit(1)

        bands = load_image_bands(image_entry)
        loaded_chunk.append(bands)

    logging.info(f"Successfully loaded {len(loaded_chunk)} images in chunk")
    return loaded_chunk


def load_image_bands(image_entry: dict[str, str]) -> dict[str, np.ndarray]:
    """
    Loads all bands for a single image entry with strict validation.

    Exit codes:
      1 = permanent failure (bad plan / missing files)
      2 = retryable IO failure (NAS visibility / transient read error)
    """
    base_name = image_entry.get("base_name")
    if not base_name:
        logging.fatal("Task chunk contains an image entry missing 'base_name'.")
        sys.exit(1)

    band_obj = image_entry.get("bands")
    if not band_obj:
        logging.error(f"Image {base_name} missing 'bands' definition.")
        sys.exit(1)

    loaded_bands = {}

    for band_name, band_path in band_obj.items():
        try:
            # Permanent: file does not exist
            if not os.path.exists(band_path):
                raise FileNotFoundError(f"Missing band file: {band_path}")
            if not os.path.isfile(band_path):
                raise FileNotFoundError(f"Not a file: {band_path}")

            img = cv2.imread(band_path, cv2.IMREAD_UNCHANGED)

            # Retryable: NAS-mounted files may transiently fail even if file exists
            if img is None:
                raise BlockingIOError(f"Band unreadable (yet): {band_path}")

            loaded_bands[band_name] = img

        except FileNotFoundError as e:
            logging.error(f"Band {band_name} missing for {base_name}: {e}")
            sys.exit(1)

        except BlockingIOError as e:
            logging.warning(f"Retryable IO error loading band {band_name} for {base_name}: {e}")
            sys.exit(2)

        except Exception as e:
            logging.error(f"Unexpected error loading band {band_name} for {base_name}")
            sys.exit(1)

    logging.info(f"Successfully loaded {len(loaded_bands)} bands for {base_name}")
    return loaded_bands

def save_multispectral_image(image: dict[str, np.ndarray], 
                             fname: str,
                             path: str,
                             extn: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    bands = list(image.keys())
    for band in bands:
        fpath_out = os.path.join(path, f"{fname}_{band}"+extn)
        cv2.imwrite(fpath_out, image[band])
    
    return f"DONEIMG:{fname}"

def concurrent_saver_thread(q: Queue, results_dir: str):
    """
    Background thread that consumes aligned images from a queue 
    and saves them in batches to maximize I/O throughput.
    """
    def _flush_save_batch(batch, results_dir):
        """Internal helper to handle the ThreadPool saving logic."""
        # max_workers=16 as per your original logic
        with ThreadPoolExecutor(max_workers=16) as ex:
            futures = [
                ex.submit(
                    save_multispectral_image,
                    image["bands"],
                    image["fname_base"],
                    results_dir,
                    image["extension"]
                ) for image in batch
            ]

            # Wait for this batch to hit the disk before clearing from memory
            for future in as_completed(futures):
                try:
                    res = future.result()
                    # logging.info(res) # Optional: verify save status
                except Exception as e:
                    pass # logging.error(f"Save failed: {e}")
                
    batch = []
    
    while True:
        item = q.get()
        
        if item is None:
            if batch:
                _flush_save_batch(batch, results_dir)
            q.task_done()
            break
        
        batch.append(item)
        if len(batch) >= SAVING_BATCH_SIZE:
            _flush_save_batch(batch, results_dir)
            batch = []
            
        q.task_done()