#!python

import asyncio
import logging
import configparser
import argparse
import sys


from merge import run_merge
from db import Run


async def main():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    parser = argparse.ArgumentParser(description='Sofia standalone execution.')
    parser.add_argument('--name', dest='name', type=str, required=True,
                        help='unique run name')
    parser.add_argument('--spatial_extent', dest='spatial', nargs='+', type=int, required=True,
                        help='sanity threshold for spatial extents (min%% max%%)')
    parser.add_argument('--spectral_extent', dest='spectral', nargs='+', type=int, required=True,
                        help='sanity threshold for spectral extents (min%% max%%)')
    parser.add_argument('--flux', dest='flux', type=int, required=True,
                        help='sanity threshold for flux (%%)')
    parser.add_argument('-c', '--conf', dest='conf', required=True,
                        help='configuration file')
    parser.add_argument('-p', '--param', dest='param', nargs='+', required=True,
                        help='sofia parameter file')

    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.conf)
    processes = config['Sofia']['processes']

    sanity = {'flux': args.flux,
              'spatial_extent': tuple(args.spatial),
              'spectral_extent': tuple(args.spectral)}

    Run.check_inputs(sanity)

    task_list = [asyncio.create_task(run_merge(config, args.name, args.param, sanity))
                 for _ in range(int(processes))]

    try:
        await asyncio.gather(*task_list)
    except Exception as e:
        logging.exception(e)
        sys.exit(1)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
