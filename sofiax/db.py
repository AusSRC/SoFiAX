import json


class Const(object):
    FULL_SCHEMA = {
        "name" : None,
        "x": None,
        "y": None,
        "z": None,
        "x_min": None,
        "x_max": None,
        "y_min": None,
        "z_min": None,
        "z_max": None,
        "n_pix": None,
        "f_min": None,
        "f_max": None,
        "f_sum": None,
        "rel": None,
        "rms": None,
        "w20": None,
        "w50": None,
        "ell_maj": None,
        "ell_min": None,
        "ell_pa": None,
        "ell3s_maj": None,
        "ell3s_pa": None,
        "kin_pa": None,
        "ra": None,
        "dec": None,
        "l": None,
        "b": None,
        "v_rad": None,
        "v_opt": None,
        "v_app": None,
        "err_x": None,
        "err_y": None,
        "err_z": None,
        "err_f_sum": None,
        "freq": None,
        "flag": None
    }

    VO_DATALINK_URL = 'https://wallaby.aussrc.org/wallaby/vo/dl/dlmeta?ID='


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
            uncertainty_sigma = sanity_thresholds['uncertainty_sigma']
            if not isinstance(uncertainty_sigma, int):
                raise ValueError('uncertainty_sigma in sanity_thresholds is not an int')
            if uncertainty_sigma <= 0:
                raise ValueError('uncertainty_sigma in sanity_thresholds is <= 0')
        except KeyError:
            raise ValueError('uncertainty_sigma missing from sanity_thresholds')

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
    run_id = await conn.fetchrow('INSERT INTO wallaby.run (name, sanity_thresholds) '
                                 'VALUES($1, $2) ON CONFLICT (name, sanity_thresholds) '
                                 'DO UPDATE SET name=EXCLUDED.name RETURNING id',
                                 run.name, json.dumps(run.sanity_thresholds))
    run.run_id = run_id[0]
    return run


async def db_instance_upsert(conn, instance: Instance):
    ins_id = await conn.fetchrow('INSERT INTO wallaby.instance (run_id, run_date, filename, boundary, flag_log, '
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


async def db_source_match(conn, run_id: int, detection: dict, uncertainty_sigma: int):
    x = detection['x']
    y = detection['y']
    z = detection['z']
    err_x = detection['err_x']
    err_y = detection['err_y']
    err_z = detection['err_z']
    result = await conn.fetch('SELECT d.id, d.instance_id, x, y, z, f_sum, ell_maj, ell_min, w50, w20, '
                              'flag, unresolved FROM wallaby.detection as d, wallaby.instance as i WHERE '
                              'ST_3DDistance(geometry(ST_MakePoint($1, $2, 0)), geometry(ST_MakePoint(x, y, 0))) '
                              f'<= {uncertainty_sigma} * SQRT((($1 - x)^2 * ($4^2 + err_x^2) + ($2 - y)^2 * ($5^2 + err_y^2)) / '
                              'COALESCE(NULLIF((($1 - x)^2 + ($2 - y)^2), 0), 1)) AND '
                              'ST_3DDistance(geometry(ST_MakePoint(0, 0, $3)), geometry(ST_MakePoint(0, 0, z))) '
                              f'<= {uncertainty_sigma} * SQRT($6 ^ 2 + err_z ^ 2) AND '
                              'd.instance_id = i.id AND i.run_id = $7 ORDER BY d.id ASC FOR UPDATE OF d',
                              x, y, z, err_x, err_y, err_z, run_id)
    for i, j in enumerate(result):
        # do not want the original detection if it already exists
        if j['x'] == x and j['y'] == y and j['z'] == z:
            result.pop(i)
            break
    return result


async def db_detection_product_insert(conn, detection_id, cube, mask, mom0, mom1, mom2, chan, spec):
    product_id = await conn.fetchrow('INSERT INTO wallaby.products '
                                     '(detection_id, cube, mask, moment0, moment1, moment2, channels, spectrum) '
                                     'VALUES($1, $2, $3, $4, $5, $6, $7, $8) '
                                     'ON CONFLICT (detection_id) '
                                     'DO UPDATE SET detection_id=EXCLUDED.detection_id RETURNING id',
                                     detection_id, cube, mask, mom0, mom1, mom2, chan, spec)

    return product_id[0]


async def db_detection_insert(conn, run_id: int, instance_id: int, detection: dict,
                              cube: bytes, mask: bytes, mom0: bytes, mom1: bytes, mom2: bytes, chan: bytes, spec: bytes,
                              unresolved: bool = False):

    detection['run_id'] = run_id
    detection['instance_id'] = instance_id
    detection['unresolved'] = unresolved

    for _, key in enumerate(Const.FULL_SCHEMA):
        if detection.get(key, None) is None:
            detection[key] = Const.FULL_SCHEMA[key]

    detection_id = await conn.fetchrow('INSERT INTO wallaby.detection '
                                       '(run_id, instance_id, unresolved, name, x, y, z, x_min, x_max, '
                                       'y_min, y_max, z_min, z_max, n_pix, f_min, f_max, f_sum, rel, flag, rms, '
                                       'w20, w50, ell_maj, ell_min, ell_pa, ell3s_maj, ell3s_min, ell3s_pa, kin_pa, '
                                       'err_x, err_y, err_z, err_f_sum, ra, dec, freq, l, b, v_rad, v_opt, v_app, '
                                       'access_url) '
                                       'VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, '
                                       '$16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, '
                                       '$28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, '
                                       '$42 || currval(pg_get_serial_sequence(\'wallaby.detection\', \'id\'))) '
                                       'ON CONFLICT (name, x, y, z, x_min, x_max, y_min, y_max, z_min, z_max, '
                                       'n_pix, f_min, f_max, f_sum, instance_id, run_id) '
                                       'DO UPDATE SET ra=EXCLUDED.ra RETURNING id',
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
                                       Const.VO_DATALINK_URL)

    await db_detection_product_insert(conn, detection_id[0], cube, mask, mom0, mom1, mom2, chan, spec)
    return detection_id[0]


async def db_delete_detection(conn, detection_id: int):
    await conn.fetchrow('DELETE FROM wallaby.detection WHERE id=$1', detection_id)


async def db_update_detection_unresolved(conn, unresolved: bool, detection_id_list: list):
    await conn.fetchrow('UPDATE wallaby.detection SET unresolved=$1 '
                        'WHERE id = ANY($2::bigint[])',
                        unresolved, detection_id_list)

