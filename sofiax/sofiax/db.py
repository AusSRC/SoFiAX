import json


async def db_observation_insert(conn, name: str):
    obs_id = await conn.fetchrow('INSERT INTO "Observation" (name) '
                                 'VALUES($1) ON CONFLICT (name) '
                                 'DO UPDATE SET name=EXCLUDED.name RETURNING id',
                                 name)
    return obs_id[0]


async def db_run_insert(conn, name: str, obs_id: int, sanity_thresholds: json):
    run_id = await conn.fetchrow('INSERT INTO "Run" (name, obs_id, sanity_thresholds) '
                                 'VALUES($1, $2, $3) ON CONFLICT (name, obs_id, sanity_thresholds) '
                                 'DO UPDATE SET name=EXCLUDED.name RETURNING id',
                                 name, obs_id, sanity_thresholds)
    return run_id[0]


async def db_instance_insert(conn, run_id, run_date, filename, boundary, flag_log,
                             reliability_plot, log, parameters):
    ins_id = await conn.fetchrow('INSERT INTO "Instance" (run_id, run_date, filename, boundary, flag_log, '
                                 'reliability_plot, log, parameters) '
                                 'VALUES($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (run_id, filename, boundary) '
                                 'DO UPDATE SET run_id=EXCLUDED.run_id RETURNING id',
                                 run_id, run_date, filename, boundary, flag_log,
                                 reliability_plot, log, parameters)
    return ins_id[0]


async def db_source_match(conn, run_id: int, detection: list):
    x = detection[1]
    y = detection[2]
    z = detection[3]
    err_x = detection[25]
    err_y = detection[26]
    err_z = detection[27]
    result = await conn.fetch('SELECT d.id, d.instance_id, x, y, z, f_sum, ell_maj, ell_min, w50, w20, '
                              'flag, unresolved FROM "Detection" as d, "Instance" as i WHERE '
                              'ST_3DDistance(geometry(ST_MakePoint($1, $2, 0)), geometry(ST_MakePoint(x, y, 0))) '
                              '<= 3 * SQRT((($1 - x)^2 * ($4^2 + err_x^2) + ($2 - y)^2 * ($5^2 + err_y^2)) / '
                              '(($1 - x)^2 + ($2 - y)^2)) AND '
                              'ST_3DDistance(geometry(ST_MakePoint(0, 0, $3)), geometry(ST_MakePoint(0, 0, z))) '
                              '<= 3 * SQRT($6 ^ 2 + err_z ^ 2) AND '
                              'x != $1 AND y != $2 AND z != $3 AND '
                              'd.instance_id = i.id AND i.run_id = $7 FOR UPDATE OF d',
                              x, y, z, err_x, err_y, err_z, run_id)
    return result


async def db_detection_product_insert(conn, detection_id, cube, mask, mom0, mom1, mom2, chan, spec):
    product_id = await conn.fetchrow('INSERT INTO "Products" '
                                     '(detection_id, cube, mask, moment0, moment1, moment2, channels, spectrum) '
                                     'VALUES($1, $2, $3, $4, $5, $6, $7, $8) '
                                     'ON CONFLICT (detection_id) '
                                     'DO UPDATE SET detection_id=EXCLUDED.detection_id RETURNING id',
                                     detection_id, cube, mask, mom0, mom1, mom2, chan, spec)

    return product_id[0]


async def db_detection_insert(conn, run_id: int, instance_id: int, detection: list,
                              cube: bytes, mask: bytes, mom0: bytes, mom1: bytes, mom2: bytes, chan: bytes, spec: bytes,
                              unresolved: bool = False):
    detection_id = await conn.fetchrow('INSERT INTO "Detection" '
                                       '(run_id, instance_id, unresolved, name, x, y, z, x_min, x_max, '
                                       'y_min, y_max, z_min, z_max, n_pix, f_min, f_max, f_sum, rel, flag, rms, '
                                       'w20, w50, ell_maj, ell_min, ell_pa, ell3s_maj, ell3s_min, ell3s_ps, err_x, '
                                       'err_y, err_z, err_f_sum, ra, dec, freq) '
                                       'VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, '
                                       '$16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, '
                                       '$28, $29, $30, $31, $32, $33, $34, $35) '
                                       'ON CONFLICT (ra, dec, freq, instance_id, run_id) '
                                       'DO UPDATE SET ra=EXCLUDED.ra RETURNING id',
                                       run_id, instance_id, unresolved, *detection)

    await db_detection_product_insert(conn, detection_id[0], cube, mask, mom0, mom1, mom2, chan, spec)
    return detection_id[0]


async def db_delete_detection(conn, detection_id: int):
    await conn.fetchrow('DELETE FROM "Detection" WHERE id=$1', detection_id)


async def db_update_detection_unresolved(conn, unresolved: bool, detection_id_list: list):
    await conn.fetchrow('UPDATE "Detection" SET unresolved=$1 '
                        'WHERE id = ANY($2::bigint[])',
                        unresolved, detection_id_list)

