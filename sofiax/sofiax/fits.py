import json
import asyncio
import astropy

from astropy.io import fits


async def extract_fits_header(filepath, loop=None):
    if not loop:
        loop = asyncio.get_event_loop()
    handle = await loop.run_in_executor(None, fits.open, filepath)
    hdr_dict = {}
    hdr = handle[0].header
    for key in hdr.keys():
        value = hdr[key]
        if isinstance(value, astropy.io.fits.header._HeaderCommentaryCards):
            value = [i for i in value]
        hdr_dict[key] = value
    return json.dumps(hdr_dict)
