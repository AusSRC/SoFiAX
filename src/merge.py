"""Merge strategy.

"""

import xmltodict
import logging
from datetime import datetime

from src.schema import Run, Instance
from src.utils.sql import db_instance_upsert, \
    db_detection_insert, db_source_match, \
    db_delete_detection, db_update_detection_unresolved
from src.utils.io import _get_parameter, _get_output_filename, \
    _get_file_bytes
from src.utils.calcs import sanity_check, _distance_from_cube_boundary


async def merge_match_detection(conn, run: Run, instance: Instance, cwd: str):
    """!Merge or match detections.

    Strategy for handling new detections. If a duplicate is identified with
    existing database entries, updates are automatically applied based on
    the heuristics implemented in the body of this function (no return).
    If there is no match the detection is merged by default.

    Args:
        conn:       Database connection object
        run         Run object...
        instance    Instance object...
        cwd         Current working directory.
    """
    # Retrieve output catalogue of SoFiA run
    output_dir = _get_parameter('output.directory', instance.params, cwd)
    output_filename = _get_output_filename(instance.params, cwd)
    vo_table = f"{output_dir}/{output_filename}_cat.xml"
    content = await _get_file_bytes(vo_table, mode='r')
    cat = xmltodict.parse(content)

    # Getting instance metadata from outputs
    run_date = None
    # NOTE(austin): why enumerate? Could just go "for j in cat[...]"?
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

    # Instance is written to database
    instance.run_date = datetime.strptime(run_date, '%a, %d %b %Y, %H:%M:%S')
    instance.reliability_plot = await _get_file_bytes(f"{output_dir}/{output_filename}_rel.eps")
    instance = await db_instance_upsert(conn, instance)

    detect_names = []
    fields = cat['VOTABLE']['RESOURCE']['TABLE']['FIELD']
    for _, j in enumerate(fields):
        detect_names.append(j['@name'])

    # Iterate through content of the output catalogue
    # header of the table
    tr = list(cat['VOTABLE']['RESOURCE']['TABLE']['DATA']['TABLEDATA']['TR'])
    for _, j in enumerate(tr):
        detect_dict = {}

        # row of table
        for i, item in enumerate(j['TD']):
            try:
                detect_dict[detect_names[i]] = float(item)
            except ValueError:
                detect_dict[detect_names[i]] = item

        # only check 0 or 4 flagged detections, throw the others away
        # automatic flags set on the data (in the catalogue)
        flag = detect_dict['flag']
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
        base_filename = f"{output_dir}/{output_filename}_cubelets/{output_filename}_{detect_id}"
        cube_bytes = await _get_file_bytes(f"{base_filename}_cube.fits")
        mask_bytes = await _get_file_bytes(f"{base_filename}_mask.fits")
        mom0_bytes = await _get_file_bytes(f"{base_filename}_mom0.fits")
        mom1_bytes = await _get_file_bytes(f"{base_filename}_mom1.fits")
        mom2_bytes = await _get_file_bytes(f"{base_filename}_mom2.fits")
        chan_bytes = await _get_file_bytes(f"{base_filename}_chan.fits")
        spec_bytes = await _get_file_bytes(f"{base_filename}_spec.txt")

        # TODO(austin): read about transactions
        async with conn.transaction():
            # result is a list of conflicting sources.
            result = await db_source_match(conn, run.run_id, detect_dict, run.sanity_thresholds['uncertainty_sigma'])

            # No detections, enter detection into database
            if len(result) == 0:
                logging.info(f"No duplicates, Name: {detect_dict['name']}")
                await db_detection_insert(conn, run.run_id, instance.instance_id, detect_dict,
                                          cube_bytes, mask_bytes, mom0_bytes, mom1_bytes,
                                          mom2_bytes, chan_bytes, spec_bytes)

            # Handle duplicate detections
            else:
                logging.info(f"Duplicates, Name: {detect_dict['name']} Details: {len(result)} hit(s)")
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

                        # Select detection based on distance to boundary.
                        # TODO(austin): Write tests to verify this works as expected.
                        elif detect_flag == 0 and db_detect_flag == 0 or detect_flag == 4 and db_detect_flag == 4:
                            candidate_dist = _distance_from_cube_boundary(detect_dict, instance.boundary)
                            current_dist = _distance_from_cube_boundary(db_detect, db_detect['boundary'])
                            if candidate_dist > current_dist:
                                logging.info(f"Replacing, Name: {detect_dict['name']} Details: flag 0 with "
                                             f"flag 0 or flag 4 with flag 4")
                                await db_delete_detection(conn, db_detect['id'])
                                await db_detection_insert(conn, run.run_id, instance.instance_id, detect_dict,
                                                          cube_bytes, mask_bytes, mom0_bytes, mom1_bytes,
                                                          mom2_bytes, chan_bytes, spec_bytes,
                                                          db_detect['unresolved'])

                        # TODO(austin): Automated method for checking if detection is component of another detection?

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

            # TODO(austin): Source name check?
            # Here we can write another query
