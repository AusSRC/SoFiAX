import json


class Run(object):
    def __init__(self, name, sanity_thresholds):
        self.run_id = None
        self.name = name
        self.sanity_thresholds = sanity_thresholds
        Run.check_inputs(self.sanity_thresholds)

    @staticmethod
    def check_inputs(sanity_thresholds: dict):
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


class Instance(object):
    def __init__(self, run_id, run_date, filename, boundary, flag_log, reliability_plot, log, parameters,
                 version, return_code, stdout, stderr):
        self.instance_id = None
        self.run_id = run_id
        self.run_date = run_date
        self.filename = filename
        self.boundary = boundary
        self.flag_log = flag_log
        self.reliability_plot = reliability_plot
        self.log = log
        self.params = parameters
        self.version = version
        self.return_code = return_code
        self.stdout = stdout
        self.stderr = stderr


async def db_run_upsert(conn, run: Run):
    run_id = await conn.fetchrow('INSERT INTO "Run" (name, sanity_thresholds) '
                                 'VALUES($1, $2) ON CONFLICT (name, sanity_thresholds) '
                                 'DO UPDATE SET name=EXCLUDED.name RETURNING id',
                                 run.name, json.dumps(run.sanity_thresholds))
    run.run_id = run_id[0]
    return run


async def db_instance_upsert(conn, instance: Instance):
    ins_id = await conn.fetchrow('INSERT INTO "Instance" (run_id, run_date, filename, boundary, flag_log, '
                                 'reliability_plot, log, parameters, version, return_code, stdout, stderr) '
                                 'VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) '
                                 'ON CONFLICT (run_id, filename, boundary) '
                                 'DO UPDATE SET '
                                 'run_date=EXCLUDED.run_date,'
                                 'filename=EXCLUDED.filename,'
                                 'boundary=EXCLUDED.boundary,'
                                 'flag_log=EXCLUDED.flag_log,'
                                 'reliability_plot=EXCLUDED.reliability_plot,'
                                 'log=EXCLUDED.log,'
                                 'parameters=EXCLUDED.parameters, '
                                 'version=EXCLUDED.version, '
                                 'return_code=EXCLUDED.return_code, '
                                 'stdout=EXCLUDED.stdout, '
                                 'stderr=EXCLUDED.stderr '
                                 'RETURNING id',
                                 instance.run_id, instance.run_date, instance.filename, instance.boundary,
                                 instance.flag_log, instance.reliability_plot, instance.log,
                                 json.dumps(instance.params), instance.version, instance.return_code,
                                 instance.stdout, instance.stderr)
    instance.instance_id = ins_id[0]
    return instance


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
                              'COALESCE(NULLIF((($1 - x)^2 + ($2 - y)^2), 0), 1)) AND '
                              'ST_3DDistance(geometry(ST_MakePoint(0, 0, $3)), geometry(ST_MakePoint(0, 0, z))) '
                              '<= 3 * SQRT($6 ^ 2 + err_z ^ 2) AND '
                              'd.instance_id = i.id AND i.run_id = $7 ORDER BY d.id ASC FOR UPDATE OF d',
                              x, y, z, err_x, err_y, err_z, run_id)
    for i, j in enumerate(result):
        # do not want the original detection if it already exists
        if j['x'] == x and j['y'] == y and j['z'] == z:
            result.pop(i)
            break
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
                                       'w20, w50, ell_maj, ell_min, ell_pa, ell3s_maj, ell3s_min, ell3s_ps, kin_pa, '
                                       'err_x, err_y, err_z, err_f_sum, ra, dec, freq) '
                                       'VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, '
                                       '$16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, '
                                       '$28, $29, $30, $31, $32, $33, $34, $35, $36) '
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

