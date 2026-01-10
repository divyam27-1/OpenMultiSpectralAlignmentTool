import numpy as np

def get_cam_mtx(band_meta: dict) -> np.ndarray:
    """
    Returns the camera intrinsic matrix.
    """
    fx, fy, cx, cy, *_ = band_meta["DewarpData"]
    Cx = band_meta["CalibratedOpticalCenterX"]
    Cy = band_meta["CalibratedOpticalCenterY"]

    cam_matrix = np.array([
        [fx,  0, Cx - cx],
        [ 0, fy, Cy - cy],
        [ 0,  0,      1]
    ], dtype=np.float64)

    return cam_matrix

def get_dist_coeff(band_meta: dict) -> np.ndarray:
    """
    Returns distortion coefficients in OpenCV order:
    [k1, k2, p1, p2, k3]
    """
    _, _, _, _, k1, k2, p1, p2, k3 = band_meta["DewarpData"]

    return np.array([k1, k2, p1, p2, k3], dtype=np.float64)
