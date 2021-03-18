import os
import aiofiles
import configparser
from astropy.io import fits


async def _parse_sofia_param_file(sofia_param_path: str):
    """!Read raw SoFiA parameter file into a dictionary.

    @param sofia_param_path     Absolute path to the SoFiA parameter file

    Returns:
        - params (dict):            SoFiA parameter file values as a Python
                                    dictionary
    """
    content_bytes = await _get_file_bytes(sofia_param_path, mode='r')
    file_contents = f"[dummy_section]\n{content_bytes}"

    # Detectionruct params from file contents
    params = {}
    config = configparser.RawConfigParser()
    config.read_string(file_contents)
    for key in config['dummy_section']:
        params[key] = config['dummy_section'][key]
    return params


async def _get_file_bytes(path: str, mode: str = 'rb'):
    """!Read a file as bytes.

    """
    buffer = []

    async with aiofiles.open(path, mode) as f:
        while True:
            buff = await f.read()
            if not buff:
                break
            buffer.append(buff)
        if 'b' in mode:
            return b''.join(buffer)
        else:
            return ''.join(buffer)


def _get_from_conf(conf, name: str, *args, **kwargs):
    """!Get value from configuration file.

    Raise ValueError if value is None.
    """
    value = conf.get(name, *args, **kwargs)
    if value is None:
        raise ValueError(f'{name} is not defined in configuration.')
    return value


def _get_parameter(name: str, params: dict, cwd: str):
    """!Retrieve value from SoFiA parameter file.

    Prepends current working directory if it is not absolute.
    """
    param = params[name]
    if os.path.isabs(param) is False:
        param = f"{cwd}/{os.path.basename(param)}"
    return param


def _get_output_filename(params: dict, cwd: str):
    """!Retrieve output filename from SoFiA parmameter file.

    """
    input_fits = _get_parameter('input.data', params, cwd)
    output_filename = params['output.filename']
    if not output_filename:
        output_filename = os.path.splitext(os.path.basename(input_fits))[0]
    return output_filename


def _get_fits_header_property(file: str, property: str):
    """!Read FITS file header and metadata.

    """
    if not os.path.isfile(file):
        raise ValueError("File does not exist.")

    hdul = fits.open(file)
    return hdul[0].header[property]
