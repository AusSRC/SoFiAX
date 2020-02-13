import os
import json
import random
import aiofiles
import xmltodict
import configparser
import logging

from datetime import datetime

from db import db_run_insert, db_instance_insert, \
    db_detection_insert, db_source_match, \
    db_delete_detection, db_update_detection_unresolved


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


def sanity_check(flux: tuple, spatial_extent: tuple, spectral_extent: tuple, sanity_thresholds: dict):
    f1, f2 = flux
    diff = abs(f1 - f2) * 100 / ((abs(f2) + abs(f2)) / 2)
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


async def match_merge_detections(conn, run_name: str, params: dict, sanity_thresholds: dict, cwd: str):
    try:
        flux = sanity_thresholds['flux']
        if not isinstance(flux, int):
            raise ValueError('flux in sanity_thresholds is not an int')
    except KeyError:
        raise ValueError('flux missing from sanity_thresholds')

    try:
        spatial = sanity_thresholds['spatial_extent']
        if not isinstance(spatial, tuple):
            raise ValueError('spatial_extent in sanity_thresholds is not a tuple')
        if len(spatial) != 2:
            raise ValueError('spatial_extent in sanity_thresholds is not a tuple of len(2)')
    except KeyError:
        raise ValueError('spatial_extent missing from sanity_thresholds')

    try:
        spectral = sanity_thresholds['spectral_extent']
        if not isinstance(spectral, tuple):
            raise ValueError('spectral_extent in sanity_thresholds is not a tuple')
        if len(spectral) != 2:
            raise ValueError('spectral_extent in sanity_thresholds is not a tuple of len(2)')
    except KeyError:
        raise ValueError('spectral_extent missing from sanity_thresholds')

    input_fits = params['input.data']
    output_dir = params['output.directory']

    if os.path.isabs(input_fits) is False:
        input_fits = f"{cwd}/{os.path.basename(input_fits)}"

    if os.path.isabs(output_dir) is False:
        output_dir = f"{cwd}/{os.path.basename(output_dir)}"

    run_id = await db_run_insert(conn, run_name, json.dumps(sanity_thresholds))

    output_filename = params['output.filename']
    if not output_filename:
        output_filename = os.path.splitext(os.path.basename(input_fits))[0]

    # sofia reliability plot
    sofia_reliability = await _get_file_bytes(f"{output_dir}/{output_filename}_rel.eps")

    vo_table = f"{output_dir}/{output_filename}_cat.xml"
    content = await _get_file_bytes(vo_table, mode='r')
    o = xmltodict.parse(content)

    run_date = None
    for _, j in enumerate(o['VOTABLE']['RESOURCE']['PARAM']):
        if j['@name'] == 'Time':
            run_date = j['@value']
            break

    if run_date is None:
        raise AttributeError('Run date not found in votable')

    sofia_boundary = [int(i) for i in params['input.region'].split(',')]
    run_date_datetime = datetime.strptime(run_date, '%a, %d %b %Y, %H:%M:%S')

    instance_id = await db_instance_insert(conn, run_id, run_date_datetime, output_filename, sofia_boundary,
                                           None, sofia_reliability, None, json.dumps(params))

    for _, j in enumerate(o['VOTABLE']['RESOURCE']['TABLE']['DATA']['TABLEDATA']['TR']):
        detection = j['TD']
        flag = int(detection[16])
        # only check 0 or 4 flagged detections, throw the others away
        if flag not in [0, 4]:
            continue
        # remove id from detection list
        detect_id = detection[1]
        del detection[1]
        # cast strings onto values
        for i in range(1, len(detection)):
            detection[i] = float(detection[i])

        # adjust x, y, z to absolute terms based on region applied
        detection[1] = detection[1] + sofia_boundary[0]
        detection[2] = detection[2] + sofia_boundary[2]
        detection[3] = detection[3] + sofia_boundary[4]

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
            result = await db_source_match(conn, run_id, detection)
            result_len = len(result)
            if result_len == 0:
                logging.info(f"No duplicates, Name: {detection[0]}")
                await db_detection_insert(conn, run_id, instance_id, detection,
                                          cube_bytes, mask_bytes, mom0_bytes, mom1_bytes,
                                          mom2_bytes, chan_bytes, spec_bytes)
            else:
                logging.info(f"Duplicates, Name: {detection[0]} Details: {result_len} hit(s)")
                resolved = False
                for db_detect in result:
                    flux = (detection[13], db_detect['f_sum'])
                    spatial = (detection[19], db_detect['ell_maj'],
                               detection[20], db_detect['ell_min'])
                    spectral = (detection[17], db_detect['w20'],
                                detection[18], db_detect['w50'])
                    check_result = sanity_check(flux, spatial, spectral, sanity_thresholds)
                    if check_result:
                        detect_flag = detection[15]
                        db_detect_flag = db_detect['flag']
                        if detect_flag == 0 and db_detect_flag == 4:
                            logging.info(f"Replacing, Name: {detection[0]} Details: flag 4 with flag 0")
                            await db_delete_detection(conn, db_detect['id'])
                            await db_detection_insert(conn, run_id, instance_id, detection, cube_bytes,
                                                      mask_bytes, mom0_bytes, mom1_bytes,
                                                      mom2_bytes, chan_bytes, spec_bytes,
                                                      db_detect['unresolved'])
                        elif detect_flag == 0 and db_detect_flag == 0 or detect_flag == 4 and db_detect_flag == 4:
                            if bool(random.getrandbits(1)) is True:
                                logging.info(f"Replacing, Name: {detection[0]} Details: flag 0 with "
                                             f"flag 0 or flag 4 with flag 4")
                                await db_delete_detection(conn, db_detect['id'])
                                await db_detection_insert(conn, run_id, instance_id, detection,
                                                          cube_bytes, mask_bytes, mom0_bytes, mom1_bytes,
                                                          mom2_bytes, chan_bytes, spec_bytes,
                                                          db_detect['unresolved'])
                        resolved = True
                        break
                if resolved is False:
                    logging.info(f"Not Resolved, Name: {detection[0]} Details: Setting to unresolved")
                    await db_detection_insert(conn, run_id, instance_id, detection,
                                              cube_bytes, mask_bytes, mom0_bytes,
                                              mom1_bytes, mom2_bytes, chan_bytes, spec_bytes,
                                              True)
                    await db_update_detection_unresolved(conn, True, [i['id'] for i in result])
