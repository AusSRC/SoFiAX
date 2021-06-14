#
# Copyright (c) 2021 AusSRC.
#
# This file is part of SoFiAX
# (see https://github.com/AusSRC/SoFiAX).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 2.1 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.#

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
