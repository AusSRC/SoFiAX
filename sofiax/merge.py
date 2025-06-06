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

import os
import glob
import random
import shutil
import asyncio
import aiofiles
import aiofiles.os
import xmltodict
import configparser
import logging
import asyncpg

from datetime import datetime

from sofiax.db import db_run_upsert, db_instance_upsert, \
    db_detection_insert, db_source_match, \
    db_delete_detection, db_update_detection_unresolved, db_lock_run, Run, Instance

from sofiax.fits import extract_fits_header


async def _get_file_bytes(path: str, mode: str = 'rb'):
    buffer = []

    if not os.path.isfile(path):
        return b''

    async with aiofiles.open(path, mode) as f:
        while True:
            buff = await f.read()
            if not buff:
                break
            buffer.append(buff)
        if 'b' in mode:
            return b''.join(buffer)
        else:
            return ''.join(buffer)


async def parse_sofia_param_file(sofia_param_path: str):
    content = await _get_file_bytes(sofia_param_path, mode='r')
    if not content:
        raise Exception(f"{sofia_param_path} is empty")
    file_contents = f"[dummy_section]\n{content}"

    params = {}
    config = configparser.RawConfigParser()
    config.read_string(file_contents)
    for key in config['dummy_section']:
        params[key] = config['dummy_section'][key]
    return params


def remove_files(path: str):
    file_list = glob.glob(path)
    for file_path in file_list:
        if os.path.isdir(file_path):
            shutil.rmtree(file_path, ignore_errors=True)
        else:
            os.remove(file_path)


async def remove_output(params: dict, cwd: str):
    input_fits = params['input.data']
    output_dir = params['output.directory']
    output_filename = params['output.filename']

    if os.path.isabs(output_dir) is False:
        output_dir = f"{cwd}/{os.path.basename(output_dir)}"

    if not output_filename:
        output_filename = os.path.splitext(os.path.basename(input_fits))[0]

    path = f"{output_dir}/{output_filename}*"
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, remove_files, path)


def sanity_check(flux: tuple, spatial_extent: tuple,
                 spectral_extent: tuple, sanity_thresholds: dict):
    f1, f2 = flux
    diff = abs(f1 - f2) * 100 / ((abs(f1) + abs(f2)) / 2)
    # gone beyond the % tolerance
    if diff > sanity_thresholds['flux']:
        message = f"Var: {f1}, {f2}, flux {round(diff, 2)}% > {sanity_thresholds['flux']}%"
        logging.info(message)
        # require manual separation, add ref to UnresolvedDetection
        return False

    min_extent, max_extent = sanity_thresholds['spatial_extent']
    max1, max2, min1, min2 = spatial_extent
    max_diff = abs(max1 - max2) * 100 / ((abs(max1) + abs(max2)) / 2)
    min_diff = abs(min1 - min2) * 100 / ((abs(min1) + abs(min2)) / 2)
    if max_diff > max_extent:
        message = f"Var: ell_maj Check: {round(max_diff, 2)}% > {max_extent}%"
        logging.info(message)
        # require manual separation, add ref to UnresolvedDetection
        return False

    if min_diff > min_extent:
        message = f"Var: ell_min Check: {round(max_diff, 2)}% > {min_extent}%"
        logging.info(message)

        # require manual separation, add ref to UnresolvedDetection
        return False

    min_extent, max_extent = sanity_thresholds['spectral_extent']
    max1, max2, min1, min2 = spectral_extent
    max_diff = abs(max1 - max2) * 100 / ((abs(max1) + abs(max2)) / 2)
    min_diff = abs(min1 - min2) * 100 / ((abs(min1) + abs(min2)) / 2)
    if max_diff > max_extent:
        message = f"Var: w20 Check: {round(max_diff, 2)}% > {max_extent}%"
        logging.info(message)
        # require manual separation, add ref to UnresolvedDetection
        return False

    if min_diff > min_extent:
        message = f"Var: w50 Check: {round(max_diff, 2)}% > {min_extent}%"
        logging.info(message)

        # require manual separation, add ref to UnresolvedDetection
        return False

    return True


async def match_merge_detections(conn, schema: str, vo_datalink_url: str,
                                 run: Run, instance: Instance, cwd: str,
                                 perform_merge: int,
                                 quality_flags: list):
    """The database connection remains open for the duration of this
    process of merging and matching detections.

    """
    input_fits = instance.params['input.data']
    output_dir = instance.params['output.directory']

    if os.path.isabs(input_fits) is False:
        input_fits = f"{cwd}/{os.path.basename(input_fits)}"

    if os.path.isabs(output_dir) is False:
        output_dir = f"{cwd}/{os.path.basename(output_dir)}"

    output_filename = instance.params['output.filename']
    if not output_filename:
        output_filename = os.path.splitext(os.path.basename(input_fits))[0]

    vo_table = f"{output_dir}/{output_filename}_cat.xml"
    if not os.path.exists(vo_table):
        raise AttributeError(f'SoFiA output catalog file {vo_table} does not exist')

    content = await _get_file_bytes(vo_table, mode='r')
    cat = xmltodict.parse(content)

    run_date = None
    for _, j in enumerate(cat['VOTABLE']['RESOURCE']['PARAM']):
        if j['@name'] == 'Time':
            run_date = j['@value']
            break

    if run_date is None:
        raise AttributeError('Run date not found in votable')

    for _, j in enumerate(cat['VOTABLE']['RESOURCE']['PARAM']):
        if j['@name'] == 'Creator':
            instance.version = j['@value']
            break

    instance.run_date = datetime.strptime(run_date, '%a, %d %b %Y, %H:%M:%S')
    instance.reliability_plot = await _get_file_bytes(
        f"{output_dir}/{output_filename}_rel.eps")

    # Lock the entire run for an instance to run exclusively
    async with conn.transaction():
        await db_lock_run(conn, schema, run)

        instance = await db_instance_upsert(conn, schema, instance)

        detect_names = []
        fields = cat['VOTABLE']['RESOURCE']['TABLE']['FIELD']
        for _, j in enumerate(fields):
            detect_names.append(j['@name'])

        tr = cat['VOTABLE']['RESOURCE']['TABLE']['DATA']['TABLEDATA']['TR']
        if not isinstance(tr, list):
            tr = [tr]

        for _, j in enumerate(tr):
            detect_dict = {}
            for i, item in enumerate(j['TD']):
                try:
                    # NOTE: handle cases where field contains "nan"
                    if "nan" in item:
                        detect_dict[detect_names[i]] = None
                    else:
                        detect_dict[detect_names[i]] = float(item)
                except ValueError:
                    detect_dict[detect_names[i]] = item

            # only allow selected flagged detections (default 0 or 4), throw the others away
            flag = detect_dict['flag']
            if flag not in quality_flags:
                continue

            # remove id from detection list
            detect_id = int(detect_dict['id'])
            del detect_dict['id']

            # adjust x, y, z to absolute terms based on region applied
            detect_dict['x'] = detect_dict['x'] + instance.boundary[0]
            detect_dict['y'] = detect_dict['y'] + instance.boundary[2]
            detect_dict['z'] = detect_dict['z'] + instance.boundary[4]

            base = f"{output_dir}/{output_filename}_cubelets/{output_filename}_{detect_id}"  # noqa

            cube_bytes = await _get_file_bytes(f"{base}_cube.fits")
            mask_bytes = await _get_file_bytes(f"{base}_mask.fits")
            mom0_bytes = await _get_file_bytes(f"{base}_mom0.fits")
            mom1_bytes = await _get_file_bytes(f"{base}_mom1.fits")
            mom2_bytes = await _get_file_bytes(f"{base}_mom2.fits")
            # NOTE: cubelet _chan.fits files renames _snr.fits in SoFiA-2 v2.3
            chan_bytes = await _get_file_bytes(f"{base}_snr.fits")
            spec_bytes = await _get_file_bytes(f"{base}_spec.txt")
            pv_bytes = await _get_file_bytes(f"{base}_pv.fits")

            # Do not merge the sources into the run, just do a direct import
            if perform_merge == 0:
                logging.info(f"Not performing merge, doing direct import. Name: {detect_dict['name']}")

                await db_detection_insert(
                        conn, schema, vo_datalink_url, run.run_id, instance.instance_id,
                        detect_dict, cube_bytes, mask_bytes, mom0_bytes, mom1_bytes,
                        mom2_bytes, chan_bytes, spec_bytes, pv_bytes, False)
                # move onto the next source
                continue

            result = await db_source_match(
                conn, schema, run.run_id, detect_dict,
                run.sanity_thresholds['uncertainty_sigma'])

            result_len = len(result)
            if result_len == 0:
                logging.info(f"No duplicates, Name: {detect_dict['name']}")
                await db_detection_insert(
                    conn, schema, vo_datalink_url, run.run_id, instance.instance_id,
                    detect_dict, cube_bytes, mask_bytes, mom0_bytes, mom1_bytes,
                    mom2_bytes, chan_bytes, spec_bytes, pv_bytes)
            else:
                logging.info(
                    f"Duplicates, Name: {detect_dict['name']} Details: {result_len} hit(s)")

                resolved = False
                for db_detect in result:
                    flux = (detect_dict['f_sum'], db_detect['f_sum'])
                    spatial = (detect_dict['ell_maj'], db_detect['ell_maj'],
                               detect_dict['ell_min'], db_detect['ell_min'])
                    spectral = (detect_dict['w20'], db_detect['w20'],
                                detect_dict['w50'], db_detect['w50'])

                    check_result = sanity_check(
                        flux, spatial, spectral, run.sanity_thresholds)

                    if check_result:
                        detect_flag = detect_dict['flag']
                        db_detect_flag = db_detect['flag']
                        if detect_flag == 0 and db_detect_flag == 4:
                            logging.info(
                                f"Replacing, Name: {detect_dict['name']} Details: flag 4 with flag 0")

                            await db_delete_detection(conn, schema, db_detect['id'])
                            await db_detection_insert(
                                conn, schema, vo_datalink_url, run.run_id, instance.instance_id,
                                detect_dict, cube_bytes, mask_bytes,
                                mom0_bytes, mom1_bytes, mom2_bytes,
                                chan_bytes, spec_bytes, pv_bytes, db_detect['unresolved'])

                        elif detect_flag == 0 and db_detect_flag == 0 or detect_flag == 4 and db_detect_flag == 4:  # noqa
                            if bool(random.getrandbits(1)) is True:
                                logging.info(
                                    f"Replacing, Name: {detect_dict['name']} Details: flag 0 with flag 0 or flag 4 with flag 4")

                                await db_delete_detection(
                                    conn, schema, db_detect['id'])

                                await db_detection_insert(
                                    conn, schema, vo_datalink_url, run.run_id, instance.instance_id,
                                    detect_dict, cube_bytes, mask_bytes,
                                    mom0_bytes, mom1_bytes, mom2_bytes,
                                    chan_bytes, spec_bytes, pv_bytes,
                                    db_detect['unresolved'])

                        resolved = True
                        break

                if resolved is False:
                    logging.info(f"Not Resolved, Name: {detect_dict['name']} Details: Setting to unresolved")

                    await db_detection_insert(
                        conn, schema, vo_datalink_url, run.run_id, instance.instance_id, detect_dict,
                        cube_bytes, mask_bytes, mom0_bytes, mom1_bytes,
                        mom2_bytes, chan_bytes, spec_bytes, pv_bytes, True)

                    await db_update_detection_unresolved(
                        conn,
                        schema,
                        True,
                        [i['id'] for i in result])


async def run_merge(config, run_name, param_list, sanity, quality_flags):
    schema = config.get('db_schema', 'wallaby')
    host = config['db_hostname']
    name = config['db_name']
    username = config['db_username']
    password = config['db_password']
    port = config['db_port']

    execute = int(config['sofia_execute'])
    path = config['sofia_path']
    vo_datalink_url = f'https://{schema}.aussrc.org/survey/vo/dl/dlmeta?ID='

    while len(param_list) > 0:
        param_path = param_list.pop(0)

        logging.info(f'*** Processing {param_path} ***')
        params = await parse_sofia_param_file(param_path)
        param_cwd = os.path.dirname(os.path.abspath(param_path))

        input_fits = params['input.data']

        region = params.get('input.region', None)
        if not region:
            header = await extract_fits_header(input_fits)

            x_max = int(header.get('NAXIS1'))
            y_max = int(header.get('NAXIS2'))

            freq_axis_1 = header.get('CTYPE3', None)
            freq_axis_2 = header.get('CTYPE4', None)

            if freq_axis_1:
                freq_axis_1 = freq_axis_1.strip()

            if freq_axis_2:
                freq_axis_2 = freq_axis_2.strip()

            if freq_axis_1 == 'FREQ':
                freq_axis = 'NAXIS3'

            if freq_axis_2 == 'FREQ':
                freq_axis = 'NAXIS4'

            z_max = int(header.get(freq_axis))

            boundary = [0, x_max-1, 0, y_max-1, 0, z_max-1]
        else:
            boundary = [int(i) for i in params['input.region'].split(',')]

        if os.path.isabs(input_fits) is False:
            input_fits = f"{param_cwd}/{os.path.basename(input_fits)}"

        output_filename = params['output.filename']
        if not output_filename:
            output_filename = os.path.splitext(os.path.basename(input_fits))[0]

        run_date = datetime.now()

        # Write run and instance to database
        conn = await asyncpg.connect(
            user=username,
            password=password,
            database=name,
            host=host,
            port=port
        )

        try:
            run = Run(run_name, sanity)
            run = await db_run_upsert(conn, schema, run)
            instance = Instance(
                run.run_id, run_date, output_filename, boundary, None, None,
                None, params, None, None, None, None)

            instance = await db_instance_upsert(conn, schema, instance)
        finally:
            await conn.close()

        # Execute sofia (if applicable)
        if execute == 1:
            logging.info(f'Executing SoFiA {param_path}')

            output_path = os.path.abspath(params['output.directory'])
            await aiofiles.os.makedirs(output_path, exist_ok=True)

            sofia_clean = int(config.get("sofia_clean", 0))
            if sofia_clean == 1:
                await remove_output(params, param_cwd)

            sofia_cmd = f'{path} {param_path}'
            proc = await asyncio.create_subprocess_shell(
                sofia_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={'SOFIA2_PATH': os.path.dirname(path)},
                cwd=param_cwd)

            stdout, stderr = await proc.communicate()
            instance.stdout = stdout
            instance.stderr = stderr
            instance.return_code = proc.returncode

        # Write detections to database
        conn = await asyncpg.connect(
            user=username,
            password=password,
            database=name,
            host=host,
            port=port
        )

        try:
            if instance.return_code == 0 or instance.return_code is None:
                perform_merge = int(config.get("perform_merge", 1))

                logging.info(f'SoFiA already completed: {param_path}')
                await match_merge_detections(conn, schema, vo_datalink_url,
                                             run, instance, param_cwd,
                                             perform_merge, quality_flags)
            else:
                code = instance.return_code
                err = f'SoFiA completed with return code: {code}'
                await db_instance_upsert(conn, schema, instance)

                logging.error(err)
                logging.error(instance.stderr)

                # no source(s) found, gracefully exit
                if instance.return_code == 8:
                    return

                raise SystemError(err)
        finally:
            await conn.close()
