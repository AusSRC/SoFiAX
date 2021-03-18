"""@package sofiax.utils
Helper functions for the sofiax package.

Additional functions used for running SoFiAX separated...
"""


def _percentage_difference(v1, v2):
    """Function to calculate the percentage difference between two values.
    Used for comparing spatial and spectral extents.

    """
    return abs(v1 - v2) * 100 / ((abs(v1) + abs(v2)) / 2)
