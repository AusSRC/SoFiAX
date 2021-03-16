import asyncio
import logging
import configparser
import argparse
import sys

from .merge import run_merge
from .database import Run, Const
from .utils import _get_from_conf


async def _main():
    """The main function.

    """
    # establish logger
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    # argument parsing
    parser = argparse.ArgumentParser(description='Sofia standalone execution.')
    parser.add_argument('-c', '--conf', dest='conf', required=True,
                        help='configuration file')
    parser.add_argument('-p', '--param', dest='param', nargs='+', required=True,
                        help='sofia parameter file')
    args = parser.parse_args()

    # get information from configuration
    config = configparser.ConfigParser()
    config.read(args.conf)
    conf = config['SoFiAX']

    processes = _get_from_conf(conf, 'sofia_processes', 0)
    run_name = _get_from_conf(conf, 'run_name', None)
    spatial = _get_from_conf(conf, 'spatial_extent', None).replace(' ', '').split(',')
    spectral = _get_from_conf(conf, 'spectral_extent', None).replace(' ', '').split(',')
    flux = int(_get_from_conf(conf, 'flux', None))
    uncertainty_sigma = _get_from_conf(conf, 'uncertainty_sigma', 5)
    vo_datalink_url = conf.get('vo_datalink_url', None)
    if vo_datalink_url is not None:
        Const.VO_DATALINK_URL = vo_datalink_url

    # wot is going on here
    sanity = {
        'flux': flux,
        'spatial_extent': tuple(map(int, spatial)),
        'spectral_extent': tuple(map(int, spectral)),
        'uncertainty_sigma': int(uncertainty_sigma)
    }

    Run.check_inputs(sanity)

    task_list = [
        asyncio.create_task(run_merge(config, run_name, args.param, sanity)) for _ in range(int(processes))
    ]

    # and here?
    try:
        await asyncio.gather(*task_list)
    except Exception as e:
        logging.exception(e)
        sys.exit(1)


def main():
    """

    """
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_main())
