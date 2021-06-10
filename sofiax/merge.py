import os
import glob
import random
import shutil
import asyncio
import aiofiles
import xmltodict
import configparser
import logging
import asyncpg

from datetime import datetime

from sofiax.db import db_run_upsert, db_instance_upsert, \
    db_detection_insert, db_source_match, \
    db_delete_detection, db_update_detection_unresolved, Run, Instance


async def _get_file_bytes(path: str, mode: str = 'rb'):
    buffer = []

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
    file_contents = f"[dummy_section]\n{await _get_file_bytes(sofia_param_path, mode='r')}"

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


def sanity_check(flux: tuple, spatial_extent: tuple, spectral_extent: tuple, sanity_thresholds: dict):
    f1, f2 = flux
    diff = abs(f1 - f2) * 100 / ((abs(f1) + abs(f2)) / 2)
    # gone beyond the % tolerance
    if diff > sanity_thresholds['flux']:
        # require manual separation, add ref to UnresolvedDetection
        return False

    min_extent, max_extent = sanity_thresholds['spatial_extent']
    max1, max2, min1, min2 = spatial_extent
    max_diff = abs(max1 - max2) * 100 / ((abs(max1) + abs(max2)) / 2)
    min_diff = abs(min1 - min2) * 100 / ((abs(min1) + abs(min2)) / 2)
    if max_diff > max_extent:
        # require manual separation, add ref to UnresolvedDetection
        return False

    if min_diff > min_extent:
        # require manual separation, add ref to UnresolvedDetection
        return False

    min_extent, max_extent = sanity_thresholds['spectral_extent']
    max1, max2, min1, min2 = spectral_extent
    max_diff = abs(max1 - max2) * 100 / ((abs(max1) + abs(max2)) / 2)
    min_diff = abs(min1 - min2) * 100 / ((abs(min1) + abs(min2)) / 2)
    if max_diff > max_extent:
        # require manual separation, add ref to UnresolvedDetection
        return False

    if min_diff > min_extent:
        # require manual separation, add ref to UnresolvedDetection
        return False

    return True


async def match_merge_detections(conn, run: Run, instance: Instance, cwd: str):

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
    instance.reliability_plot = await _get_file_bytes(f"{output_dir}/{output_filename}_rel.eps")

    instance = await db_instance_upsert(conn, instance)

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
                detect_dict[detect_names[i]] = float(item)
            except ValueError:
                detect_dict[detect_names[i]] = item

        flag = detect_dict['flag']
        # only check 0 or 4 flagged detections, throw the others away
        if flag not in [0, 4]:
            continue

        # remove id from detection list
        detect_id = int(detect_dict['id'])
        del detect_dict['id']

        # adjust x, y, z to absolute terms based on region applied
        detect_dict['x'] = detect_dict['x'] + instance.boundary[0]
        detect_dict['y'] = detect_dict['y'] + instance.boundary[2]
        detect_dict['z'] = detect_dict['z'] + instance.boundary[4]

        path = f"{output_dir}/{output_filename}_cubelets/{output_filename}_{detect_id}_cube.fits"
        cube_bytes = await _get_file_bytes(path)
        path = f"{output_dir}/{output_filename}_cubelets/{output_filename}_{detect_id}_mask.fits"
        mask_bytes = await _get_file_bytes(path)
        path = f"{output_dir}/{output_filename}_cubelets/{output_filename}_{detect_id}_mom0.fits"
        mom0_bytes = await _get_file_bytes(path)
        path = f"{output_dir}/{output_filename}_cubelets/{output_filename}_{detect_id}_mom1.fits"
        mom1_bytes = await _get_file_bytes(path)
        path = f"{output_dir}/{output_filename}_cubelets/{output_filename}_{detect_id}_mom2.fits"
        mom2_bytes = await _get_file_bytes(path)
        path = f"{output_dir}/{output_filename}_cubelets/{output_filename}_{detect_id}_chan.fits"
        chan_bytes = await _get_file_bytes(path)
        path = f"{output_dir}/{output_filename}_cubelets/{output_filename}_{detect_id}_spec.txt"
        spec_bytes = await _get_file_bytes(path)

        async with conn.transaction():
            result = await db_source_match(conn, run.run_id, detect_dict, run.sanity_thresholds['uncertainty_sigma'])
            result_len = len(result)
            if result_len == 0:
                logging.info(f"No duplicates, Name: {detect_dict['name']}")
                await db_detection_insert(conn, run.run_id, instance.instance_id, detect_dict,
                                          cube_bytes, mask_bytes, mom0_bytes, mom1_bytes,
                                          mom2_bytes, chan_bytes, spec_bytes)
            else:
                logging.info(f"Duplicates, Name: {detect_dict['name']} Details: {result_len} hit(s)")
                resolved = False
                for db_detect in result:
                    flux = (detect_dict['f_sum'], db_detect['f_sum'])
                    spatial = (detect_dict['ell_maj'], db_detect['ell_maj'],
                               detect_dict['ell_min'], db_detect['ell_min'])
                    spectral = (detect_dict['w20'], db_detect['w20'],
                                detect_dict['w50'], db_detect['w50'])
                    check_result = sanity_check(flux, spatial, spectral, run.sanity_thresholds)
                    if check_result:
                        detect_flag = detect_dict['flag']
                        db_detect_flag = db_detect['flag']
                        if detect_flag == 0 and db_detect_flag == 4:
                            logging.info(f"Replacing, Name: {detect_dict['name']} Details: flag 4 with flag 0")
                            await db_delete_detection(conn, db_detect['id'])
                            await db_detection_insert(conn, run.run_id, instance.instance_id, detect_dict, cube_bytes,
                                                      mask_bytes, mom0_bytes, mom1_bytes,
                                                      mom2_bytes, chan_bytes, spec_bytes,
                                                      db_detect['unresolved'])
                        elif detect_flag == 0 and db_detect_flag == 0 or detect_flag == 4 and db_detect_flag == 4:
                            if bool(random.getrandbits(1)) is True:
                                logging.info(f"Replacing, Name: {detect_dict['name']} Details: flag 0 with "
                                             f"flag 0 or flag 4 with flag 4")
                                await db_delete_detection(conn, db_detect['id'])
                                await db_detection_insert(conn, run.run_id, instance.instance_id, detect_dict,
                                                          cube_bytes, mask_bytes, mom0_bytes, mom1_bytes,
                                                          mom2_bytes, chan_bytes, spec_bytes,
                                                          db_detect['unresolved'])
                        resolved = True
                        break
                if resolved is False:
                    logging.info(f"Not Resolved, Name: {detect_dict['name']} Details: Setting to unresolved")
                    await db_detection_insert(conn, run.run_id, instance.instance_id, detect_dict,
                                              cube_bytes, mask_bytes, mom0_bytes,
                                              mom1_bytes, mom2_bytes, chan_bytes, spec_bytes,
                                              True)
                    await db_update_detection_unresolved(conn, True, [i['id'] for i in result])


async def run_merge(config, run_name, param_list, sanity):
    conf = config['SoFiAX']
    host = conf['db_hostname']
    name = conf['db_name']
    username = conf['db_username']
    password = conf['db_password']

    execute = int(conf['sofia_execute'])
    path = conf['sofia_path']

    while len(param_list) > 0:
        param_path = param_list.pop(0)

        logging.info(f'Processing {param_path}')
        params = await parse_sofia_param_file(param_path)
        param_cwd = os.path.dirname(os.path.abspath(param_path))

        input_fits = params['input.data']
        boundary = [int(i) for i in params['input.region'].split(',')]

        if os.path.isabs(input_fits) is False:
            input_fits = f"{param_cwd}/{os.path.basename(input_fits)}"

        output_filename = params['output.filename']
        if not output_filename:
            output_filename = os.path.splitext(os.path.basename(input_fits))[0]

        run_date = datetime.now()

        conn = await asyncpg.connect(user=username, password=password, database=name, host=host)
        try:
            run = Run(run_name, sanity)
            run = await db_run_upsert(conn, run)
            instance = Instance(run.run_id, run_date, output_filename, boundary, None, None, None,
                                params, None, None, None, None)
            instance = await db_instance_upsert(conn, instance)
        finally:
            await conn.close()

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
            instance.stdout = stdout
            instance.stderr = stderr
            instance.return_code = proc.returncode

        conn = await asyncpg.connect(user=username, password=password, database=name, host=host)
        try:
            if instance.return_code == 0 or instance.return_code is None:
                logging.info(f'Sofia completed: {param_path}')
                await match_merge_detections(conn, run, instance, param_cwd)
            else:
                err = f'Sofia completed with return code: {instance.return_code}'
                await db_instance_upsert(conn, instance)

                logging.error(err)
                logging.error(instance.stderr)

                # no source(s) found, gracefully exit
                if instance.return_code == 8:
                    return

                raise SystemError(err)

        finally:
            await conn.close()
