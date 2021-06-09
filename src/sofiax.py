"""@package sofiax
SoFiAX execution class.

SoFiAX is a repository of code to take outputs of SoFiA-2 and store the
content in databases.
"""

import os
import asyncio
import asyncpg
import logging
import configparser
import sys
from datetime import datetime

from src.schema import Run, Detection, Instance
from src.merge import merge_match_detection
from src.utils.io import _get_output_filename, \
    _parse_sofia_param_file, _get_from_conf, _get_parameter, \
    _get_fits_header_property
from src.utils.sql import db_run_upsert, db_instance_upsert


class SoFiAX:
    """!SoFiAX execution class

    Manages the configuration, logging, and execution of SoFiAX. Stores
    database credentials and SoFiA parameters.
    """
    def __init__(self, config: str, params: str):
        """!Constructor for SoFiAX executor.

        @param config   SoFiAX configuration file path
        @param params   SoFiA parameter file(s) path as comma delimited string
        """
        # Member variables (config)
        self.config = None
        self.db_conn = None
        self.sofia_path = None
        self.sofia_execute = False
        self.sofia_processes = None
        self.run_name = None
        self.sanity = None

        # Member variables (params)
        self.sofia_params = []

        # Initialisation
        self._init_logger()
        self._init_parse_config(config)
        self._init_check_sofia_params(params)

    def _init_logger(self):
        """!Set up SoFiAX logger.

        """
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    async def _init_db_connection(self):
        """!Connect to the database.

        """
        self.db_conn = await asyncpg.connect(
            user=self.config['db_username'],
            password=self.config['db_password'],
            database=self.config['db_name'],
            host=self.config['db_hostname']
        )

    async def _terminate_db_connection(self):
        """!Close connection to the database.

        """
        await self.db_conn.close()

    def _init_parse_config(self, path: str):
        """!Parse SoFiAX configuration

        Populates the values of member variables with content
        from the configuation. This includes the run name, the
        sanity threshold values.

        @param path     SoFiAX configuration file path

        @return         Dictionary of configuration file
                        content.
        """
        # Check config file exists
        if not os.path.isfile(path):
            raise ValueError(
                f"SoFiAX configuration file at '{path}' does not exist."
            )

        # Parse config file
        config_parser = configparser.ConfigParser()
        config_parser.read(path)
        config = config_parser['SoFiAX']
        self.config = config

        # Get SoFiA executable values from config
        self.sofia_path = _get_from_conf(config, 'run_name', None)
        self.sofia_execute = bool(int(config['sofia_execute']))
        self.sofia_processes = int(
            _get_from_conf(config, 'sofia_processes', 0)
        )

        # Get sanity check values from config
        self.run_name = _get_from_conf(config, 'run_name', None)
        spatial = _get_from_conf(config, 'spatial_extent', None)\
            .replace(' ', '').split(',')
        spectral = _get_from_conf(config, 'spectral_extent', None)\
            .replace(' ', '').split(',')
        flux = int(_get_from_conf(config, 'flux', None))
        uncertainty_sigma = _get_from_conf(config, 'uncertainty_sigma', 5)
        vo_datalink_url = config.get('vo_datalink_url', None)
        if vo_datalink_url is not None:
            Detection.VO_DATALINK_URL = vo_datalink_url
        self.sanity = {
            'flux': flux,
            'spatial_extent': tuple(map(int, spatial)),
            'spectral_extent': tuple(map(int, spectral)),
            'uncertainty_sigma': int(uncertainty_sigma)
        }

        # Check sanity thresholds
        Run.check_inputs(self.sanity)

    def _init_check_sofia_params(self, params):
        """!Check SoFiA parameter file(s)

        Check that each SoFiA parameter file that has been passed exists.
        Add to list of files to execute.

        @param params   SoFiA parameter file path(s)
        """
        for param_file in params:
            if not os.path.isfile(param_file):
                raise ValueError(
                    f"SoFiA parameter file at '{param_file}' does not exist."
                )
            self.sofia_params.append(param_file)

    async def _upsert_run_subprocess(self, boundary, params, output_filename):
        """!Update/insert Run and Instance into database.

        """
        run = Run(self.run_name, self.sanity)
        run = await db_run_upsert(self.db_conn, run)
        instance = Instance(run.run_id, datetime.now(), output_filename,
                            boundary, None, None, None,
                            params, None, None, None, None)
        instance = await db_instance_upsert(self.db_conn, instance)
        return run, instance

    async def _run_sofia_subprocess(self, instance, sofia_param_file, cwd):
        """!Run SoFiA as a subprocess.

        Log status of SoFiA run upon completion.

        """
        if self.sofia_execute:
            logging.info(f'Executing SoFiA {sofia_param_file}')
            sofia_cmd = f'{self.sofia_path} {sofia_param_file}'
            proc = await asyncio.create_subprocess_shell(
                sofia_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={'SOFIA2_PATH': os.path.dirname(self.sofia_path)},
                cwd=cwd)
            stdout, stderr = await proc.communicate()
            instance.stdout = stdout
            instance.stderr = stderr
            instance.return_code = proc.returncode

            if instance.return_code == 0 or instance.return_code is None:
                logging.info(f'Sofia completed: {sofia_param_file}')
            else:
                err = f'Sofia completed with return code: \
                    {instance.return_code}'
                await db_instance_upsert(self.db_conn, instance)

                logging.error(err)
                logging.error(instance.stderr)

                # no source(s) found, gracefully exit
                if instance.return_code == 8:
                    return
                raise SystemError(err)

    async def process(self):
        """!Contents of a SoFiAX process.

        Describes the work that is done by SoFiAX. Currently,
        the process contains the following stages:

        1. Read sofia parameter file
        2. Get inputs and outputs for SoFiAX run
        3. Update/insert Run details into the database
        4. Run SoFiA (if configured to do so)
        5. Run merge/match check

        SoFiAX users can add additional functionality to a SoFiAX run here.
        """
        while len(self.sofia_params) > 0:
            # Read SoFiA parameter file to dictionary
            sofia_param_file = self.sofia_params.pop(0)
            logging.info(f'Processing {sofia_param_file}')
            params = await _parse_sofia_param_file(sofia_param_file)
            cwd = os.path.dirname(os.path.abspath(sofia_param_file))

            # Get boundary
            if not params['input.region'].strip():
                input_file = _get_parameter('input.data', params, cwd)
                boundary = [
                    0, int(_get_fits_header_property(input_file, 'NAXIS1')),
                    0, int(_get_fits_header_property(input_file, 'NAXIS2')),
                    0, int(_get_fits_header_property(input_file, 'NAXIS3')),
                ]
            else:
                boundary = [int(i) for i in params['input.region'].split(',')]

            # Get output file
            output_filename = _get_output_filename(params, cwd)

            # Upsert Run into database
            run, instance = await self._upsert_run_subprocess(
                boundary,
                params,
                output_filename
            )

            # Run SoFiA (if necessary)
            await self._run_sofia_subprocess(instance, sofia_param_file, cwd)

            # Run merge / match for detections
            await merge_match_detection(self.db_conn, run, instance, cwd)

    async def execute(self):
        """!Run SoFiAX.

        Create a list of coroutines to run in an event loop. Each
        coroutine will Run all at once.

        """
        # Connect to database
        await self._init_db_connection()

        n_processes = min(self.sofia_processes, len(self.sofia_params))
        tasks = [
            asyncio.create_task(
                self.process()
            ) for _ in range(n_processes)
        ]

        # Run all tasks
        try:
            await asyncio.gather(*tasks)
            await self._terminate_db_connection()
        except Exception as e:
            logging.exception(e)
            await self._terminate_db_connection()
            sys.exit(1)
