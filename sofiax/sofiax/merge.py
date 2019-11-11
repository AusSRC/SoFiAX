import os
import json
import aiofiles
import xmltodict
import configparser

from datetime import datetime

from sofiax.db import db_observation_insert, db_run_insert, \
    db_instance_insert, db_detection_insert, db_source_match, \
    db_unresolved_insert, db_delete_detection, db_update_detection_unresolved_id


async def parse_sofia_param_file(sofia_param_path: str):
    async with aiofiles.open(sofia_param_path, mode='r') as f:
        file_contents = f"[dummy_section]\n{await f.read()}"

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
        print('spectral max failed')
        # require manual separation, add ref to UnresolvedDetection
        return False

    if min_diff > min_extent:
        print('spectral min failed')
        # require manual separation, add ref to UnresolvedDetection
        return False

    return True


async def match_merge_detections(conn, obs_name: str, run_name: str, params: dict, sanity_thresholds: dict):
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
        raise ValueError('input.data requires absolute path')

    if os.path.isabs(output_dir) is False:
        raise ValueError('output.directory requires absolute path')

    obs_id = await db_observation_insert(conn, obs_name)
    run_id = await db_run_insert(conn, run_name, obs_id, json.dumps(sanity_thresholds))

    output_filename = params['output.filename']
    if not output_filename:
        output_filename = os.path.splitext(os.path.basename(input_fits))[0]

    # sofia reliability plot
    async with aiofiles.open(f"{output_dir}/{output_filename}_rel.eps", mode='rb') as f:
        sofia_reliability = await f.read()

    vo_table = f"{output_dir}/{output_filename}_cat.xml"
    async with aiofiles.open(vo_table, mode='r') as f:
        content = await f.read()
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

        instance_id = await db_instance_insert(conn, run_id, run_date_datetime, sofia_boundary,
                                               None, sofia_reliability, None, json.dumps(params))

        for _, j in enumerate(o['VOTABLE']['RESOURCE']['TABLE']['DATA']['TABLEDATA']['TR']):
            detection = j['TD']
            flag = int(detection[16])
            # only check 0 or 4 flagged detections, throw the others away
            if flag in [0, 4]:
                # remove id from detection list
                del detection[1]
                # cast strings onto values
                for i in range(1, len(detection)):
                    detection[i] = float(detection[i])

                async with conn.transaction():
                    result = await db_source_match(conn, run_id, detection)
                    result_len = len(result)
                    if result_len == 0:
                        await db_detection_insert(conn, instance_id, detection)
                    elif result_len == 1:
                        flux = (detection[13], result[0]['f_sum'])
                        spatial = (detection[19], result[0]['ell_maj'], detection[20], result[0]['ell_min'])
                        spectral = (detection[17], result[0]['w20'], detection[18], result[0]['w50'])
                        check_result = sanity_check(flux, spatial, spectral, sanity_thresholds)
                        # if the sanity check fails, add detection and mark both as needed manual resolution
                        if check_result is False:
                            detection_id = await db_detection_insert(conn, instance_id, detection)
                            unresolved_id = result[0]['unresolved_detection_id']
                            if unresolved_id is not None:
                                await db_update_detection_unresolved_id(conn, unresolved_id, [detection_id])
                            else:
                                await db_unresolved_insert(conn, [detection_id, result[0]['id']])
                        else:
                            if detection[15] == 0 and result[0]['flag'] == 4:
                                # keep flagged detections of 0 over 4
                                await db_delete_detection(conn, result[0]['id'])
                                await db_detection_insert(conn, instance_id, detection)
                    else:
                        # if there are multiple detections then they must already be unresolved
                        detect_set = set()
                        for detect in result:
                            detect_set.add(detect['unresolved_detection_id'])
                        if len(detect_set) != 1:
                            raise Exception('Sources that are similar do not share the same unresolved detection id')

                        unresolved_id = result[0]['unresolved_detection_id']
                        detection_id = await db_detection_insert(conn, instance_id, detection)
                        await db_update_detection_unresolved_id(conn, unresolved_id, [detection_id])
