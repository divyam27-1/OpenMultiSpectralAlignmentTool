import os
import csv
import cv2
import numpy as np
import logging

from itertools import combinations
from collections import defaultdict

from typing import List, Dict

def compute_phase_correlations(samples: list[dict]) -> list[dict]:
    """
    Compute phase correlation between all band pairs for each sample
    
    Args:
        samples: List of multispectral image samples with the schema
            {
                "bands": {band_name: image_array}   (required)
                "fname_base": str,                  (optional)
                "class": str,                       (optional)
            }

    Returns:
        A list of results with the schema
            {
                "fname_base": str,
                "class": str,
                "band1": str,
                "band2": str,
                "shift_x": float,
                "shift_y": float,
                "response": float,
                "magnitude": float,
            }
    """
    results = []

    for sample in samples:
        fname_base = sample.get("fname_base", "unknown")
        image_class = sample.get("class", "unclassified")
                
        for (b1, img1), (b2, img2) in combinations(sample["bands"].items(), 2):
            try:
                f1, f2 = img1.astype(np.float32), img2.astype(np.float32)
                shift, response = cv2.phaseCorrelate(f1, f2)

                mag = float(np.hypot(shift[0], shift[1]))

                results.append({
                    "fname_base": fname_base,
                    "class": image_class,
                    "band1": b1,
                    "band2": b2,
                    "shift_x": float(shift[0]),
                    "shift_y": float(shift[1]),
                    "response": float(response),
                    "magnitude": mag,
                })
            except Exception as e:
                logging.warning(f"Phase correlation failed for {fname_base} ({b1}-{b2}): {e}")
                continue

    return results

def filter_outliers(results: list[dict]) -> list[dict]:
    """
    Filters out results that exhibit unusually large phase shifts, which are
    assumed to be artifacts of incorrect computation.

    Args:
        results: List of dictionaries representing phase-correlated results.
            Each dictionary must include a "magnitude" field (float).

    Returns:
        List of results excluding detected outliers, with the same schema as input.
    """
    magnitudes = np.array([r["magnitude"] for r in results])
    sdev = np.std(magnitudes)

    flags = magnitudes >= (sdev / 2)

    if flags.mean() < 0.1:
        logging.warning(
            f"Detected {flags.sum()} outliers out of {len(results)} samples; Outliers detected due to UB. Dropping them from results."
        )
        return [r for r, f in zip(results, flags) if not f]

    return results

def summarize_shifts(shifts: list[dict]) -> dict:
    """
    Gives statistical summary of phase shift magnitudes.

    Args:
        shifts: List of dictionaries representing phase-correlated results.
            Each dictionary must include a "magnitude" field (float).

    Returns:
        Results with the schema
            {
                "count": int,
                "mean": float,
                "median": float,
                "std": float,
            }
    """
    mags = np.array([r["magnitude"] for r in shifts])

    return {
        "count": int(len(mags)),
        "mean": float(np.mean(mags)),
        "median": float(np.median(mags)),
        "std": float(np.std(mags)),
    }

def write_phase_csv(results: list[dict], path: str) -> None:
    """
    Writes phase correlation results to a CSV file.
    
    Args:
        results: List of dictionaries representing phase-correlated results with the schema
            {
                "fname_base": str,
                "class": str,
                "band1": str,
                "band2": str,
                "shift_x": float,
                "shift_y": float,
                "response": float,
                "magnitude": float,
            }
        path: Output CSV file path
    
    Returns:
        None
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Image Fname", "Class",
            "Band1", "Band2",
            "Shift_X", "Shift_Y",
            "Response"
        ])
        
        for r in results:
            writer.writerow([
                r["fname_base"], r["class"],
                r["band1"], r["band2"],
                r["shift_x"], r["shift_y"],
                r["response"]
            ])