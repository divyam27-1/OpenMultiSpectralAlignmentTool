import cv2
import numpy as np
import logging

from typing import Tuple

def align_multispectral_sample(
    sample_data: dict[str, np.ndarray], 
    metadata_config: dict[str, dict], 
    base_band: str = None
) -> dict[str, np.ndarray]:
    """
    Aligns a single multispectral image

    This function executes a vertical pipeline: Translation -> Undistortion -> 
    De-vignetting -> Registration (ECC).

    Args:
        sample_data: A dictionary mapping band names to raw image arrays of the form
            {
                "r": np.ndarray,
                "g": np.ndarray,
                "re": np.ndarray,
                "nir": np.ndarray,
                "other_bands": {...},
                ...
            }
        metadata_config: A dictionary containing pre-calculated geometric and 
            radiometric parameters for each band with atleast the parameters
            {
                "r": {
                    "CalibratedOpticalCenterX": float,
                    "CalibratedOpticalCenterY": float,
                    "DewarpData": List[float],
                    "RelativeOpticalCenterX: float,
                    "RelativeOpticalCenterY: float,
                    "VignettingData": List[float],
                    "CameraMatrix": np.ndarray,
                    "DistCoeff": np.ndarray
                },
                "g": {...},
                "re": {...},
                "nir": {...}
                "other_bands": {...}
                ...
            }
            base_band: Must be one of the bands in sample_data. If left blank, will be first band of sample

    Returns:
        A dictionary of aligned and corrected image arrays of the form
            {
                "r": np.ndarray,
                "g": np.ndarray,
                "re": np.ndarray,
                "nir": np.ndarray,
                "other_bands": np.ndarray,
                ...
            }
    """
    # ------------------------------------------------
    # Stage 1 — Geometry (translation + undistortion + cropping)
    # ------------------------------------------------
    
    # Pre-calculate the global max shifts to ensure consistent cropping across all bands
    max_shift_x = max(abs(m["RelativeOpticalCenterX"]) for m in metadata_config.values())
    max_shift_y = max(abs(m["RelativeOpticalCenterY"]) for m in metadata_config.values())
    margin_x = int(np.ceil(max_shift_x))
    margin_y = int(np.ceil(max_shift_y))

    h, w = next(iter(sample_data.values())).shape[:2]

    geom_bands = {}
    for band, img in sample_data.items():
        m = metadata_config[band]

        img = translate_band(img, m["RelativeOpticalCenterX"], m["RelativeOpticalCenterY"])
        img = undistort_band(img, m["CameraMatrix"], m["DistCoeff"])

        img = img[margin_y : h - margin_y, 
                  margin_x : w - margin_x]

        geom_bands[band] = img

    # ------------------------------------------------
    # Stage 2 — Radiometric correction (safe now)
    # ------------------------------------------------
    radiometric_corrected_bands = {}

    for band, img in geom_bands.items():
        m = metadata_config[band]
        img = de_vignette_band(img, m["VignettingData"])
        radiometric_corrected_bands[band] = img

    # ------------------------------------------------
    # Stage 3 - ECC based alignment
    # ------------------------------------------------
    if base_band == None:
        base_band = list(sample_data.keys())[0]
    
    final_aligned_bands = {}
    ref_img = radiometric_corrected_bands[base_band]
    final_aligned_bands[base_band] = ref_img

    for band, img in radiometric_corrected_bands.items():
        if band == base_band: continue
        
        success, aligned_img = align_ecc(ref_img, img)
        if success:
            final_aligned_bands[band] = aligned_img
            continue
        
        logging.warning(f"ECC Failed for band {band}. Retrying once with more downsampling")
        retry, retry_aligned_img = align_ecc(ref_img, img, scale=0.25)

        if retry:
            final_aligned_bands[band] = retry_aligned_img
            continue
        
        logging.warning(f"ECC Failed completely for band {band}. Reverting to original image.")
        final_aligned_bands[band] = img

    # ------------------------------------------------
    # Stage 4 — Shared symmetric black-border crop
    # ------------------------------------------------
    crop_y, crop_x = compute_symmetric_crop(
        final_aligned_bands,
        black_threshold=1.0
    )

    cropped_bands = {}
    for band, img in geom_bands.items():
        cropped_bands[band] = apply_symmetric_crop(
            img, crop_y, crop_x
        )
        
    return cropped_bands


def translate_band(img: np.ndarray, dx: float, dy: float) -> np.ndarray:
    """
    Applies relative optical center translation to a single image band.
    
    Args:
        img: The input image array (typically uint16 or float32).
        dx: Horizontal translation offset (RelativeOpticalCenterX).
        dy: Vertical translation offset (RelativeOpticalCenterY).

    Returns:
        The translated image array with borders replicated.
    """
    M = np.float32([[1, 0, -dx], [0, 1, -dy]])
    return cv2.warpAffine(
        img, M, (img.shape[1], img.shape[0]),
        flags=cv2.INTER_LINEAR,
        borderMode=cv2.BORDER_REPLICATE
    )

def undistort_band(
    image: np.ndarray, 
    cam_matrix: np.ndarray, 
    dist_coeffs: np.ndarray, 
    crop_percent: float = 0.0
) -> np.ndarray:
    """
    Removes lens distortion using camera intrinsics and optional radial cropping.
    
    Args:
        image: The input image array.
        cam_matrix: 3x3 camera intrinsic matrix (K).
        dist_coeffs: Vector of distortion coefficients (k1, k2, p1, p2, k3).
        crop_percent: Percentage of the image to crop from edges to remove artifacts.

    Returns:
        The undistorted (and optionally cropped) image array.
    """
    h, w = image.shape[:2]
    map1, map2 = cv2.initUndistortRectifyMap(
        cam_matrix, dist_coeffs, None, cam_matrix, (w, h), cv2.CV_32FC1
    )
    undistorted = cv2.remap(image, map1, map2, cv2.INTER_LINEAR)
    
    if crop_percent > 0:
        ch, cw = int(h * crop_percent), int(w * crop_percent)
        undistorted = undistorted[ch:h - ch, cw:w - cw]
    return undistorted

def de_vignette_band(image: np.ndarray, vignetting_coeffs: list[float]) -> np.ndarray:
    """
    Corrects light fall-off (vignetting) based on polynomial coefficients.
    
    Args:
        image: The input image array (uint16).
        vignetting_coeffs: List of coefficients for the vignetting polynomial.

    Returns:
        Radiometrically corrected image array as uint16.
    """
    h, w = image.shape[:2]
    y, x = np.indices((h, w))
    # Distance from center calculation
    r = np.abs(x - w/2) + np.abs(y - h/2)
    mask = np.polyval([1] + vignetting_coeffs, r)       # DJI metadata does not come preloaded with constant term coefficient
    mask = np.clip(mask, 0.1, 1.0)
    
    corrected = image.astype(np.float32) / mask
    return np.clip(corrected, 0, 65535).astype(np.uint16)

def to_edges(img: np.ndarray) -> np.ndarray:
    """
    Pre-processes image for ECC alignment using Sobel gradients to emphasize features.
    
    Args:
        img: The input image array.

    Returns:
        A float32 gradient magnitude map (edges).
    """
    img_f = img.astype(np.float32)
    blur = cv2.GaussianBlur(img_f, (7, 7), 1.8)
    gx = cv2.Sobel(blur, cv2.CV_32F, 1, 0)
    gy = cv2.Sobel(blur, cv2.CV_32F, 0, 1)
    return np.abs(gx) + np.abs(gy)

def align_ecc(
    base_img: np.ndarray,
    target_img: np.ndarray,
    scale: float = 0.4,
    min_cc: float = 0.8
) -> Tuple[bool, np.ndarray]:
    """ 
    Calculates and applies Enhanced Correlation Coefficient (ECC) translation.

    Args: 
        base_img: The reference image band (e.g., NIR). 
        target_img: The band to be aligned (e.g., Red). 
        scale: Internal downscale factor to speed up the ECC search convergence. 
    
    Returns: 
        Tuple: (status, image) The target image warped to match the coordinate system of the base image.
    """

    # --- Edge extraction + downscale ---
    base_e = cv2.resize(
        to_edges(base_img), None,
        fx=scale, fy=scale, interpolation=cv2.INTER_AREA
    )
    target_e = cv2.resize(
        to_edges(target_img), None,
        fx=scale, fy=scale, interpolation=cv2.INTER_AREA
    )

    # --- Guard against textureless images (important) ---
    if np.std(base_e) < 1e-6 or np.std(target_e) < 1e-6:
        return False, target_img

    # --- Normalize ---
    base_s = base_e / (np.percentile(base_e, 95) + 1e-7)
    target_s = target_e / (np.percentile(target_e, 95) + 1e-7)

    warp_matrix = np.eye(2, 3, dtype=np.float32)
    criteria = (
        cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT,
        400,
        1e-5
    )

    try:
        # 🔹 Capture ECC score
        cc, warp_matrix = cv2.findTransformECC(
            base_s.astype(np.float32),
            target_s.astype(np.float32),
            warp_matrix,
            cv2.MOTION_TRANSLATION,
            criteria
        )
    except cv2.error:
        return False, target_img

    # 🔹 Reject weak convergence
    if cc < min_cc:
        logging.warning(f"CC less than threshold: {cc}:{min_cc}")
        return False, target_img

    # 🔹 Rescale translation back to full resolution
    warp_matrix[0, 2] /= scale
    warp_matrix[1, 2] /= scale

    aligned = cv2.warpAffine(
        target_img,
        warp_matrix,
        (target_img.shape[1], target_img.shape[0]),
        flags=cv2.INTER_LINEAR + cv2.WARP_INVERSE_MAP
    )

    return True, aligned

def crop_image(image, crop_percent):
    if crop_percent <= 0:
        return image

    h, w = image.shape[:2]
    crop_h = int(h * crop_percent)
    crop_w = int(w * crop_percent)
    cropped_image = image[crop_h:h - crop_h, crop_w:w - crop_w]

    return cropped_image

def compute_symmetric_crop(
    images: dict[str, np.ndarray],
    black_threshold: float = 0
) -> Tuple[int, int]:
    """
    Computes a single symmetric crop (crop_y, crop_x) for black edge artifacts on images

    Args:
        images: Multispectral image (dict) of the form
        {
            "band1": np.ndarray,
            "band2": np.ndarray,
            ...
        }
        black_threshold: Threshold level for black pixel (should be between 0.5 to 2 based on undistortion matrix)

    Returns:
        (crop_y, crop_x): Tuple of vertical and horizontal symmetric croppings to be done
    """
    max_top = max_bottom = max_left = max_right = 0

    for img in images.values():
        t, b, l, r = black_border_margins(img, black_threshold)

        max_top = max(max_top, t)
        max_bottom = max(max_bottom, b)
        max_left = max(max_left, l)
        max_right = max(max_right, r)

    crop_y = max(max_top, max_bottom)
    crop_x = max(max_left, max_right)

    return crop_y, crop_x

def apply_symmetric_crop(
    image: np.ndarray,
    crop_y: int,
    crop_x: int
) -> np.ndarray:
    if crop_y == 0 and crop_x == 0:
        return image

    h, w = image.shape[:2]
    return image[crop_y:h - crop_y, crop_x:w - crop_x]


def black_border_margins(image: np.ndarray, black_threshold: float = 0):
    """
    Returns (top, bottom, left, right) black margins in pixels.
    Helper function for compute_symmetric_crop
    """
    if image.ndim == 3:
        gray = np.max(image, axis=2)
    else:
        gray = image

    mask = gray > black_threshold

    if not np.any(mask):
        return 0, 0, 0, 0

    rows = mask.any(axis=1)
    cols = mask.any(axis=0)

    top = np.argmax(rows)
    bottom = np.argmax(rows[::-1])
    left = np.argmax(cols)
    right = np.argmax(cols[::-1])

    return top, bottom, left, right