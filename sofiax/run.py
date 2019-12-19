import asyncio
import asyncpg
import logging
import sys

from sofiax.merge import match_merge_detections, parse_sofia_param_file


async def run():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    conn = await asyncpg.connect(user='sofia_user', password='sofia_user',
                                 database='sofiadb', host='127.0.0.1')

    params = await parse_sofia_param_file('/Users/dpallot/Projects/SoFiAX/data/eridanus_full_3/sofia_h.par')
    sanity_thresholds = {'flux': 5, 'spatial_extent': (5, 5), 'spectral_extent': (5, 5)}
    await match_merge_detections(conn,
                                 'eridanus',
                                 'eridanus_run',
                                 params,
                                 sanity_thresholds)

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
