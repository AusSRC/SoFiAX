"""SQL commands for interacting with database.

Called via Python wrapper functions.
"""

import json
from src.schema import Run, Instance, Detection


async def db_run_upsert(conn, run: Run):
    """Add a Run object into the database.

    """
    query = 'INSERT INTO wallaby.run (name, sanity_thresholds) \
        VALUES($1, $2) ON CONFLICT (name, sanity_thresholds) \
        DO UPDATE SET name=EXCLUDED.name \
        RETURNING id'
    run_id = await conn.fetchrow(query, run.name, json.dumps(run.sanity_thresholds))
    run.run_id = run_id[0]
    return run


async def db_instance_upsert(conn, instance: Instance):
    """!SQL Instance upsert operation

    Add or update the Instance object into the database.
    """
    query = 'INSERT INTO wallaby.instance \
            (run_id, run_date, filename, boundary, flag_log, reliability_plot, \
            log, parameters, version, return_code, stdout, stderr) \
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
        ON CONFLICT (run_id, filename, boundary) \
        DO UPDATE SET \
            run_date=EXCLUDED.run_date, \
            filename=EXCLUDED.filename, \
            boundary=EXCLUDED.boundary, \
            flag_log=EXCLUDED.flag_log, \
            reliability_plot=EXCLUDED.reliability_plot, \
            log=EXCLUDED.log, \
            parameters=EXCLUDED.parameters, \
            version=EXCLUDED.version, \
            return_code=EXCLUDED.return_code, \
            stdout=EXCLUDED.stdout, \
            stderr=EXCLUDED.stderr \
        RETURNING id'
    ins_id = await conn.fetchrow(query, instance.run_id, instance.run_date,
                                 instance.filename, instance.boundary,
                                 instance.flag_log, instance.reliability_plot,
                                 instance.log, json.dumps(instance.params),
                                 instance.version, instance.return_code,
                                 instance.stdout, instance.stderr)
    instance.instance_id = ins_id[0]
    return instance


async def db_source_match(conn, run_id: int, detection: dict, uncertainty_sigma: int):
    """Check if a detection matches another detection in the database.

    """
    x = detection['x']
    y = detection['y']
    z = detection['z']
    err_x = detection['err_x']
    err_y = detection['err_y']
    err_z = detection['err_z']

    query = f'SELECT d.id, d.instance_id, i.boundary, x, y, z, f_sum, ell_maj, ell_min, w50, w20, flag, unresolved \
        FROM wallaby.detection as d, wallaby.instance as i \
        WHERE ST_3DDistance(\
                geometry(ST_MakePoint($1, $2, 0)), \
                geometry(ST_MakePoint(x, y, 0))\
            ) <= {uncertainty_sigma} * \
            SQRT((($1 - x)^2 * ($4^2 + err_x^2) + ($2 - y)^2 * ($5^2 + err_y^2)) / \
            COALESCE(NULLIF((($1 - x)^2 + ($2 - y)^2), 0), 1)) \
        AND ST_3DDistance(\
                geometry(ST_MakePoint(0, 0, $3)), \
                geometry(ST_MakePoint(0, 0, z))\
            ) <= {uncertainty_sigma} * SQRT($6 ^ 2 + err_z ^ 2) \
        AND d.instance_id = i.id \
        AND i.run_id = $7 \
        ORDER BY d.id ASC FOR UPDATE OF d'
    result = await conn.fetch(query, x, y, z, err_x, err_y, err_z, run_id)

    for i, j in enumerate(result):
        # do not want the original detection if it already exists
        if j['x'] == x and j['y'] == y and j['z'] == z:
            result.pop(i)
            break
    return result


async def db_detection_product_insert(conn, detection_id, cube, mask, mom0, mom1, mom2, chan, spec):
    query = 'INSERT INTO wallaby.products \
            (detection_id, cube, mask, moment0, moment1, moment2, channels, spectrum) \
        VALUES($1, $2, $3, $4, $5, $6, $7, $8) \
        ON CONFLICT (detection_id) \
        DO UPDATE SET detection_id=EXCLUDED.detection_id RETURNING id'
    product_id = await conn.fetchrow(query, detection_id, cube, mask, mom0, mom1, mom2, chan, spec)
    return product_id[0]


async def db_detection_insert(conn, run_id: int, instance_id: int, detection: dict,
                              cube: bytes, mask: bytes, mom0: bytes, mom1: bytes, mom2: bytes, chan: bytes, spec: bytes,
                              unresolved: bool = False):

    detection['run_id'] = run_id
    detection['instance_id'] = instance_id
    detection['unresolved'] = unresolved

    for _, key in enumerate(Detection.SCHEMA):
        if detection.get(key, None) is None:
            detection[key] = Detection.SCHEMA[key]

    query = "INSERT INTO wallaby.detection \
            (run_id, instance_id, unresolved, name, x, y, z, x_min, x_max, \
            y_min, y_max, z_min, z_max, n_pix, f_min, f_max, f_sum, rel, flag, rms, \
            w20, w50, ell_maj, ell_min, ell_pa, ell3s_maj, ell3s_min, ell3s_pa, kin_pa, \
            err_x, err_y, err_z, err_f_sum, ra, dec, freq, l, b, v_rad, v_opt, v_app, access_url) \
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, \
            $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, \
            $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, \
            $42 || currval(pg_get_serial_sequence(\'wallaby.detection\', \'id\'))) \
        ON CONFLICT (name, x, y, z, x_min, x_max, y_min, y_max, z_min, z_max, \
            n_pix, f_min, f_max, f_sum, instance_id, run_id) \
        DO UPDATE SET ra=EXCLUDED.ra RETURNING id"
    detection_id = await conn.fetchrow(query,
                                       detection['run_id'], detection['instance_id'], detection['unresolved'],
                                       detection['name'], detection['x'], detection['y'], detection['z'],
                                       detection['x_min'], detection['x_max'],
                                       detection['y_min'], detection['y_max'], detection['z_min'], detection['z_max'],
                                       detection['n_pix'], detection['f_min'], detection['f_max'], detection['f_sum'],
                                       detection['rel'], detection['flag'], detection['rms'],
                                       detection['w20'], detection['w50'], detection['ell_maj'], detection['ell_min'],
                                       detection['ell_pa'], detection['ell3s_maj'], detection['ell3s_min'],
                                       detection['ell3s_pa'], detection['kin_pa'], detection['err_x'],
                                       detection['err_y'], detection['err_z'], detection['err_f_sum'],
                                       detection['ra'], detection['dec'], detection['freq'], detection['l'],
                                       detection['b'], detection['v_rad'], detection['v_opt'], detection['v_app'],
                                       Detection.VO_DATALINK_URL)

    await db_detection_product_insert(conn, detection_id[0], cube, mask, mom0, mom1, mom2, chan, spec)
    return detection_id[0]


async def db_delete_detection(conn, detection_id: int):
    await conn.fetchrow(
        'DELETE FROM wallaby.detection WHERE id=$1',
        detection_id
    )


async def db_update_detection_unresolved(conn, unresolved: bool, detection_id_list: list):
    await conn.fetchrow(
        'UPDATE wallaby.detection SET unresolved=$1 WHERE id = ANY($2::bigint[])',
        unresolved,
        detection_id_list
    )
