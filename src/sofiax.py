"""@package sofiax
Main file for running sofiax.

SoFiAX is a repository of code to take outputs of SoFiA-2 and store the
content in databases.
"""

import os
import asyncio
import asyncpg
import logging
import configparser
import argparse
import sys
from datetime import datetime

from src.schema import Run, Detection, Instance
from src.merge import merge_match_detection
from src.utils.io import _get_output_filename, \
    _parse_sofia_param_file, _get_from_conf, _get_parameter, \
    _get_fits_header_property
from src.utils.sql import db_run_upsert, db_instance_upsert


async def run(config, run_name, param_path, sanity):
    """!Run SoFiAX and/or SoFiA and write to database.

    Write SoFiA source finding output to the database specified in the
    config file through SoFiAX. Users are allows to run SoFiA as part of
    the execution of SoFiAX.

    @param config       Config file argument
    @param run_name     Run name
    @param param_path   Location of the SoFiA parameter file (str)
    @param sanity       Dictionary of flux, spatial and spectral extent
                        values for sanity checking
    """
    # get database credentials from config
    conf = config['SoFiAX']
    host = conf['db_hostname']
    name = conf['db_name']
    username = conf['db_username']
    password = conf['db_password']
    execute_sofia = bool(int(conf['sofia_execute']))
    path = conf['sofia_path']

    logging.info(f'Processing {param_path}')
    params = await _parse_sofia_param_file(param_path)
    cwd = os.path.dirname(os.path.abspath(param_path))

    # TODO(austin): the params file allows for the specification of
    # an output directory. This needs to be included here.
    output_filename = _get_output_filename(params, cwd)

    # NOTE: SoFiA-2 can be run without the boundary specified. if the boundary
    # is none then we need to get the boundary from the input file.
    if not params['input.region'].strip():
        input_file = _get_parameter('input.data', params, cwd)
        boundary = [
            0, int(_get_fits_header_property(input_file, 'NAXIS1')),
            0, int(_get_fits_header_property(input_file, 'NAXIS2')),
            0, int(_get_fits_header_property(input_file, 'NAXIS3')),
        ]
    else:
        boundary = [int(i) for i in params['input.region'].split(',')]

    # Connect to the database and enter details of the run.
    conn = await asyncpg.connect(user=username, password=password, database=name, host=host)
    try:
        run = Run(run_name, sanity)
        run = await db_run_upsert(conn, run)
        instance = Instance(run.run_id, datetime.now(), output_filename,
                            boundary, None, None, None,
                            params, None, None, None, None)
        instance = await db_instance_upsert(conn, instance)
    finally:
        await conn.close()

    # Running SoFiA in a subprocess
    if execute_sofia:
        logging.info(f'Executing SoFiA {param_path}')
        sofia_cmd = f'{path} {param_path}'
        proc = await asyncio.create_subprocess_shell(
            sofia_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={'SOFIA2_PATH': os.path.dirname(path)},
            cwd=cwd)
        stdout, stderr = await proc.communicate()
        instance.stdout = stdout
        instance.stderr = stderr
        instance.return_code = proc.returncode

    # Take output from a completed SoFiA run
    conn = await asyncpg.connect(user=username, password=password, database=name, host=host)
    try:
        # SoFiA completed successfully, can start running SoFiAX
        if instance.return_code == 0 or instance.return_code is None:
            if execute_sofia:
                logging.info(f'SoFiA completed: {param_path}')
            await merge_match_detection(conn, run, instance, cwd)

            # TODO(austin): any other functions to run?
            # e.g. await name_match_check(conn, run, instance, cwd)...

        # Error in SoFiA run
        else:
            if execute_sofia:
                err = f'SoFiA completed with return code: {instance.return_code}'
            await db_instance_upsert(conn, instance)

            logging.error(err)
            logging.error(instance.stderr)

            # no source(s) found, gracefully exit
            if instance.return_code == 8:
                return
            raise SystemError(err)
    finally:
        await conn.close()


async def _main():
    """!Asynchronous main function.

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
                        help='SoFiA parameter file')
    args = parser.parse_args()

    # get information from configuration
    config = configparser.ConfigParser()
    config.read(args.conf)
    conf = config['SoFiAX']

    # Sofia params
    sofia_params = args.param[0]

    processes = int(_get_from_conf(conf, 'sofia_processes', 0))
    run_name = _get_from_conf(conf, 'run_name', None)
    spatial = _get_from_conf(conf, 'spatial_extent', None).replace(' ', '').split(',')
    spectral = _get_from_conf(conf, 'spectral_extent', None).replace(' ', '').split(',')
    flux = int(_get_from_conf(conf, 'flux', None))
    uncertainty_sigma = _get_from_conf(conf, 'uncertainty_sigma', 5)
    vo_datalink_url = conf.get('vo_datalink_url', None)
    if vo_datalink_url is not None:
        Detection.VO_DATALINK_URL = vo_datalink_url

    sanity = {
        'flux': flux,
        'spatial_extent': tuple(map(int, spatial)),
        'spectral_extent': tuple(map(int, spectral)),
        'uncertainty_sigma': int(uncertainty_sigma)
    }
    Run.check_inputs(sanity)

    task_list = [
        asyncio.create_task(run(config, run_name, sofia_params, sanity)) for _ in range(processes)
    ]

    try:
        await asyncio.gather(*task_list)
    except Exception as e:
        logging.exception(e)
        sys.exit(1)


def main():
    """!Main function entrypoint.

    Runs the asynchronous main function in an event loop.
    """
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_main())
