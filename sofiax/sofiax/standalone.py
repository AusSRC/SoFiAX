import asyncio
import asyncpg
import logging
import configparser
import argparse
import sys
import os

from merge import match_merge_detections, parse_sofia_param_file


async def run(config, run_name, param_list, sanity):
    host = config['Database']['hostname']
    name = config['Database']['name']
    username = config['Database']['username']
    password = config['Database']['password']

    execute = int(config['Sofia']['execute'])
    path = config['Sofia']['path']

    conn = await asyncpg.connect(user=username, password=password,
                                 database=name, host=host)

    while len(param_list) > 0:
        param_path = param_list.pop(0)

        logging.info(f'Processing {param_path}')
        params = await parse_sofia_param_file(param_path)
        param_cwd = os.path.dirname(os.path.abspath(param_path))
        if execute == 1:
            logging.info(f'Executing Sofia {param_path}')
            sofia_cmd = f'{path} {param_path}'
            proc = await asyncio.create_subprocess_shell(
                sofia_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={'SOFIA2_PATH': os.path.dirname(path)},
                cwd=param_cwd)

            stdout, stderr = await proc.communicate()
            if proc.returncode == 0:
                logging.info(f'Sofia complete {param_path}')
            else:
                err = f'Sofia completed with error: {proc.returncode}'
                logging.error(err)
                raise SystemError(err)

        await match_merge_detections(conn, run_name, params, sanity, param_cwd)


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
                        help='sanity threshold for spatial extents (%)')
    parser.add_argument('--spectral_extent', dest='spectral', nargs='+', type=int, required=True,
                        help='sanity threshold for spectral extents (%)')
    parser.add_argument('--flux', dest='flux', type=int, required=True,
                        help='sanity threshold for flux (%)')
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

    task_list = [asyncio.create_task(run(config, args.name, args.param, sanity))
                 for _ in range(int(processes))]

    await asyncio.gather(*task_list)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
