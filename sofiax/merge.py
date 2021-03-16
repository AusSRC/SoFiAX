import os
import random
import asyncio
import xmltodict
import logging
import asyncpg
from datetime import datetime

from .database import Run, Instance, \
    db_run_upsert, db_instance_upsert, \
    db_detection_insert, db_source_match, \
    db_delete_detection, db_update_detection_unresolved
from .utils import _get_parameter, _get_output_filename, _flux_difference, \
    _get_file_bytes, _parse_sofia_param_file


def sanity_check(flux: tuple, spatial_extent: tuple, spectral_extent: tuple, sanity_thresholds: dict):  # noqa
    """A function to check the spectral and spatial extent of the detection against
    minimum and maximum values. If they fall outside this range they the sanity check returns
    False. Otherwise, True.

    Args:
        flux
        spatial_extent
        spectral_extent
        sanity_thresholds

    Returns (bool):
        True  - Detection passes sanity check.
        False - Require manual separation, add
                ref to UnresolvedDetection
    """
    f1, f2 = flux
    diff = _flux_difference(f1, f2)

    # gone beyond the % tolerance
    if diff > sanity_thresholds['flux']:
        return False

    # TODO(austin): Refactor the code below into a util function.
    min_extent, max_extent = sanity_thresholds['spatial_extent']
    max1, max2, min1, min2 = spatial_extent
    max_diff = _flux_difference(max1, max2)
    min_diff = _flux_difference(min1, min2)

    if max_diff > max_extent:
        return False

    # TODO(austin): Ask why is the sign like this? Shouldn't it be
    # min_diff < min_extent?
    if min_diff > min_extent:
        return False

    min_extent, max_extent = sanity_thresholds['spectral_extent']
    max1, max2, min1, min2 = spectral_extent
    max_diff = _flux_difference(max1, max2)
    min_diff = _flux_difference(min1, min2)

    if max_diff > max_extent:
        return False

    if min_diff > min_extent:
        return False

    return True


async def match_merge_detections(conn, run: Run, instance: Instance, cwd: str):
    """Merge detections that matched.

    """
    output_dir = _get_parameter('output.directory', instance.params, cwd)
    output_filename = _get_output_filename(instance.params, cwd)

    # Retrieve output catalogue of SoFiA run
    vo_table = f"{output_dir}/{output_filename}_cat.xml"
    content = await _get_file_bytes(vo_table, mode='r')
    cat = xmltodict.parse(content)

    # Getting instance metadata from outputs
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

    # TODO(austin): What is this "tr" thing?
    # header of the table
    tr = cat['VOTABLE']['RESOURCE']['TABLE']['DATA']['TABLEDATA']['TR']
    if not isinstance(tr, list):
        tr = [tr]

    for _, j in enumerate(tr):
        detect_dict = {}
        # row of table
        for i, item in enumerate(j['TD']):
            try:
                detect_dict[detect_names[i]] = float(item)
            except ValueError:
                detect_dict[detect_names[i]] = item

        flag = detect_dict['flag']
        # only check 0 or 4 flagged detections, throw the others away
        # automatic flags set on the data (in the catalogue)
        if flag not in [0, 4]:
            continue

        # remove id from detection list
        detect_id = int(detect_dict['id'])
        del detect_dict['id']

        # adjust x, y, z to absolute terms based on region applied
        detect_dict['x'] = detect_dict['x'] + instance.boundary[0]
        detect_dict['y'] = detect_dict['y'] + instance.boundary[2]
        detect_dict['z'] = detect_dict['z'] + instance.boundary[4]

        # Individual products for each detection
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

        # TODO(austin): read about transactions
        async with conn.transaction():
            result = await db_source_match(conn, run.run_id, detect_dict, run.sanity_thresholds['uncertainty_sigma'])
            # result is a list of conflicting sources.
            result_len = len(result)

            # No detections, enter detection into database
            if result_len == 0:
                logging.info(f"No duplicates, Name: {detect_dict['name']}")
                await db_detection_insert(conn, run.run_id, instance.instance_id, detect_dict,
                                          cube_bytes, mask_bytes, mom0_bytes, mom1_bytes,
                                          mom2_bytes, chan_bytes, spec_bytes)
            # Handle duplicate detections
            else:
                logging.info(f"Duplicates, Name: {detect_dict['name']} Details: {result_len} hit(s)")
                resolved = False
                for db_detect in result:
                    flux = (detect_dict['f_sum'], db_detect['f_sum'])
                    spatial = (detect_dict['ell_maj'], db_detect['ell_maj'],
                               detect_dict['ell_min'], db_detect['ell_min'])
                    spectral = (detect_dict['w20'], db_detect['w20'],
                                detect_dict['w50'], db_detect['w50'])
                    sanity_check_satisfied = sanity_check(flux, spatial, spectral, run.sanity_thresholds)

                    if sanity_check_satisfied:
                        detect_flag = detect_dict['flag']
                        db_detect_flag = db_detect['flag']

                        # Delete automatically if flags 0 or 4
                        if detect_flag == 0 and db_detect_flag == 4:
                            logging.info(f"Replacing, Name: {detect_dict['name']} Details: flag 4 with flag 0")
                            await db_delete_detection(conn, db_detect['id'])
                            await db_detection_insert(conn, run.run_id, instance.instance_id, detect_dict, cube_bytes,
                                                      mask_bytes, mom0_bytes, mom1_bytes,
                                                      mom2_bytes, chan_bytes, spec_bytes,
                                                      db_detect['unresolved'])

                        # Random selection of detection
                        elif detect_flag == 0 and db_detect_flag == 0 or detect_flag == 4 and db_detect_flag == 4:
                            if bool(random.getrandbits(1)) is True:
                                logging.info(f"Replacing, Name: {detect_dict['name']} Details: flag 0 with "
                                             f"flag 0 or flag 4 with flag 4")
                                await db_delete_detection(conn, db_detect['id'])
                                await db_detection_insert(conn, run.run_id, instance.instance_id, detect_dict,
                                                          cube_bytes, mask_bytes, mom0_bytes, mom1_bytes,
                                                          mom2_bytes, chan_bytes, spec_bytes,
                                                          db_detect['unresolved'])
                        # Reconciled?
                        resolved = True
                        break

                # Not able to reconcile automatically, leave for manual resolution by the user.
                if resolved is False:
                    logging.info(f"Not Resolved, Name: {detect_dict['name']} Details: Setting to unresolved")
                    await db_detection_insert(conn, run.run_id, instance.instance_id, detect_dict,
                                              cube_bytes, mask_bytes, mom0_bytes,
                                              mom1_bytes, mom2_bytes, chan_bytes, spec_bytes,
                                              True)
                    await db_update_detection_unresolved(conn, True, [i['id'] for i in result])


async def run_merge(config, run_name, param_list, sanity):
    """Run SoFiA

    """

    # database configuration details
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
        params = await _parse_sofia_param_file(param_path)
        cwd = os.path.dirname(os.path.abspath(param_path))
        output_filename = _get_output_filename(params, cwd)

        input_fits = params['input.data']
        boundary = [int(i) for i in params['input.region'].split(',')]

        if os.path.isabs(input_fits) is False:
            input_fits = f"{cwd}/{os.path.basename(input_fits)}"

        output_filename = params['output.filename']
        if not output_filename:
            output_filename = os.path.splitext(os.path.basename(input_fits))[0]

        run_date = datetime.now()

        # Connect to the database and enter details of the run.
        conn = await asyncpg.connect(user=username, password=password, database=name, host=host)
        try:
            run = Run(run_name, sanity)
            run = await db_run_upsert(conn, run)
            instance = Instance(run.run_id, run_date, output_filename, boundary, None, None, None,
                                params, None, None, None, None)
            instance = await db_instance_upsert(conn, instance)
        finally:
            await conn.close()

        # Running SoFiA in a subprocess
        if execute == 1:
            logging.info(f'Executing Sofia {param_path}')

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
            # SoFiA completed successfully
            if instance.return_code == 0 or instance.return_code is None:
                logging.info(f'Sofia completed: {param_path}')
                await match_merge_detections(conn, run, instance, cwd)
            # Error in SoFiA run
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
