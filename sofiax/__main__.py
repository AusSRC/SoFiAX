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

import asyncio
import logging
import configparser
import argparse
import sys

from sofiax.utils import read_config
from sofiax.merge import run_merge
from sofiax.db import Run, Const


def logger():
    """Set up the logger.

    """
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)


def parse_args():
    """Parse arguments for the execution of SoFiAX.

    """
    parser = argparse.ArgumentParser(
        prog='SoFiAX',
        description="Sofiax standalone execution."
    )
    parser.add_argument(
        "-c",
        "--conf",
        dest="conf",
        required=True,
        help="configuration file"
    )
    parser.add_argument(
        "-p",
        "--param",
        dest="param",
        nargs="+",
        required=True,
        help="sofia parameter file"
    )
    args = parser.parse_args()
    return args


def parse_config(file):
    """Read config.ini file.

    """
    config = configparser.ConfigParser()
    config.read(file)
    return config["SoFiAX"]


async def _main():
    logger()
    args = parse_args()
    config = parse_config(args.conf)

    processes = config.get("sofia_processes", 0)
    run_name = read_config(config, "run_name")
    spatial = read_config(config, "spatial_extent")\
        .replace(" ", "").split(",")
    spectral = read_config(config, "spectral_extent")\
        .replace(" ", "").split(",")
    flux = int(read_config(config, "flux"))
    uncertainty_sigma = config.get("uncertainty_sigma", 5)
    quality_flags = list(map(int, config.get("quality_flags", "0,4")\
        .replace(" ", "").split(",")))

    sanity = {
        "flux": flux,
        "spatial_extent": tuple(map(int, spatial)),
        "spectral_extent": tuple(map(int, spectral)),
        "uncertainty_sigma": int(uncertainty_sigma)
    }

    Run.check_inputs(sanity)

    try:
        task_list = [
            asyncio.create_task(
                run_merge(config, run_name, args.param, sanity, quality_flags)
            ) for _ in range(int(processes))
        ]
        await asyncio.gather(*task_list)
    except Exception as e:
        logging.exception(e)
        sys.exit(1)


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.run_until_complete(_main())
