"""@package sofiax.utils
Helper functions for the sofiax package.

Additional functions used for running SoFiAX separated...
"""

import numpy as np


def _percentage_difference(v1, v2):
    """!Calculate percentage difference between two values.

    Used for comparing spatial and spectral extents.
    """
    return abs(v1 - v2) * 100 / ((abs(v1) + abs(v2)) / 2)


def sanity_check(flux: tuple, spatial_extent: tuple, spectral_extent: tuple, sanity_thresholds: dict):
    """!Sanity check for flux, spatial and spectral extent.

    Compare flux values, spatial and spectral extent between two detections
    that have been classified as a match. If values are unreasonable, return
    False. Otherwise, return True.

    Args:
        flux (tuple)                - Flux values to compare
        spatial_extent (tuple)      - Spatial extent of detections to compare
        spectral_extent (tuple)     - Spectral extent of detections to compare
        sanity_thresholds (dict)    - Sanity threshold dictated by the Run?

    Returns (bool):
        True  - Detection passes sanity check.
        False - Requires manual separation, add
                ref to UnresolvedDetection
    """
    # Compare flux values
    f1, f2 = flux
    flux_difference = _percentage_difference(f1, f2)
    if flux_difference > sanity_thresholds['flux']:
        return False

    # Compare spatial extent
    min_extent, max_extent = sanity_thresholds['spatial_extent']
    max1, max2, min1, min2 = spatial_extent
    max_diff = _percentage_difference(max1, max2)
    min_diff = _percentage_difference(min1, min2)

    if (max_diff > max_extent) or (min_diff > min_extent):
        return False

    # Compare spectral extent
    min_extent, max_extent = sanity_thresholds['spectral_extent']
    max1, max2, min1, min2 = spectral_extent
    max_diff = _percentage_difference(max1, max2)
    min_diff = _percentage_difference(min1, min2)

    if (max_diff > max_extent) or (min_diff > min_extent):
        return False

    return True


def _distance_from_cube_boundary(detection: dict, boundary: list):
    """!Calculate the distance of a detection from the cube boundary.

    @param detection    Detection row values.
    @param boundary     List to define boundary of sub cube
                        (xmin, xmax, ymin, ymax, zmin, zmax)

    @return distance    Distance of the detection from the subcube boundary.
    """
    if detection['x'] is None | detection['x'] is None | detection['x'] is None:
        raise ValueError("Detection coordinate values missing.")

    _min = np.array(boundary[0], boundary[2], boundary[4])
    _max = np.array(boundary[1], boundary[3], boundary[5])
    pos = np.array([detection['x'], detection['y'], detection['z']]) - _min

    return min(np.concatenate((pos, _max - pos)))
