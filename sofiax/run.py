import asyncio
import asyncpg


from sofiax.merge import match_merge_detections, parse_sofia_param_file


async def run():
    conn = await asyncpg.connect(user='sofia_user', password='sofia_user',
                                 database='sofiadb', host='127.0.0.1')

    params = await parse_sofia_param_file('/Users/dpallot/Projects/sofia/SoFiA-2/sofia.par')
    sanity_thresholds = {'flux': 2, 'spatial_extent': (2, 2), 'spectral_extent': (2, 2)}
    await match_merge_detections(conn,
                                 'image.restored.i.SB2338.V2.cube',
                                 'image.restored.i.SB2338.V2.cube_run',
                                 params,
                                 sanity_thresholds)

loop = asyncio.get_event_loop()
loop.run_until_complete(run())