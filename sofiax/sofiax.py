#!python

import asyncio
import logging
import configparser
import argparse
import sys

from merge import run_merge
from db import Run, Const


async def main():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    parser = argparse.ArgumentParser(description='Sofia standalone execution.')

    parser.add_argument('-c', '--conf', dest='conf', required=True,
                        help='configuration file')
    parser.add_argument('-p', '--param', dest='param', nargs='+', required=True,
                        help='sofia parameter file')

    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.conf)
    conf = config['SoFiAX']
    processes = conf.get('sofia_processes', 0)
    run_name = conf.get('run_name', None)
    if run_name is None:
        raise ValueError('run_name is not defined in configuration.')

    spatial = conf.get('spatial_extent', None)
    if spatial is None:
        raise ValueError('spatial_extent is not defined in configuration.')
    spatial = spatial.replace(' ', '').split(',')

    spectral = conf.get('spectral_extent', None)
    if spectral is None:
        raise ValueError('spectral_extent is not defined in configuration.')
    spectral = spectral.replace(' ', '').split(',')

    flux = conf.get('flux', None)
    if flux is None:
        raise ValueError('flux is not defined in configuration.')
    flux = int(flux)

    uncertainty_sigma = conf.get('uncertainty_sigma', 5)
    if uncertainty_sigma is None:
        raise ValueError('uncertainty_sigma is empty.')

    vo_datalink_url = conf.get('vo_datalink_url', None)
    if vo_datalink_url is not None:
        Const.VO_DATALINK_URL = vo_datalink_url

    sanity = {'flux': flux,
              'spatial_extent': tuple(map(int, spatial)),
              'spectral_extent': tuple(map(int, spectral)),
              'uncertainty_sigma': int(uncertainty_sigma)}

    Run.check_inputs(sanity)

    task_list = [asyncio.create_task(run_merge(config, run_name, args.param, sanity))
                 for _ in range(int(processes))]

    try:
        await asyncio.gather(*task_list)
    except Exception as e:
        logging.exception(e)
        sys.exit(1)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
