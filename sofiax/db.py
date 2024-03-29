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

import json
import sys
import logging


MAX_BYTEA = 1073741823


class Const(object):
    FULL_SCHEMA = {
        "name": None,
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
        "flag": None,
        "unresolved": None,
        "wm50": None,
        "x_peak": None,
        "y_peak": None,
        "z_peak": None,
        "ra_peak": None,
        "dec_peak": None,
        "freq_peak": None,
        "l_peak": None,
        "b_peak": None,
        "v_rad_peak": None,
        "v_opt_peak": None,
        "v_app_peak": None
    }


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
                raise ValueError(
                    'uncertainty_sigma in sanity_thresholds is not an int'
                )
            if uncertainty_sigma <= 0:
                raise ValueError(
                    'uncertainty_sigma in sanity_thresholds is <= 0'
                )
        except KeyError:
            raise ValueError(
                'uncertainty_sigma missing from sanity_thresholds'
            )

        try:
            spatial = sanity_thresholds['spatial_extent']
            if not isinstance(spatial, tuple):
                raise ValueError(
                    'spatial_extent in sanity_thresholds is not a tuple'
                )
            if len(spatial) != 2:
                raise ValueError(
                    'spatial_extent in sanity_thresholds \
                    is not a tuple of len(2)'
                )
        except KeyError:
            raise ValueError('spatial_extent missing from sanity_thresholds')

        try:
            spectral = sanity_thresholds['spectral_extent']
            if not isinstance(spectral, tuple):
                raise ValueError(
                    'spectral_extent in sanity_thresholds is not a tuple'
                )
            if len(spectral) != 2:
                raise ValueError(
                    'spectral_extent in sanity_thresholds \
                    is not a tuple of len(2)'
                )
        except KeyError:
            raise ValueError('spectral_extent missing from sanity_thresholds')


class Instance(object):
    def __init__(self, run_id, run_date, filename, boundary,
                 flag_log, reliability_plot, log, parameters,
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


async def db_run_upsert(conn, schema: str, run: Run):
    run_id = await conn.fetchrow(
        f'INSERT INTO {schema}.run (name, sanity_thresholds) \
        VALUES($1, $2) \
        ON CONFLICT (name) \
        DO UPDATE SET name=EXCLUDED.name \
        RETURNING id',
        run.name,
        json.dumps(run.sanity_thresholds)
    )
    run.run_id = run_id[0]
    return run


async def db_lock_run(conn, schema: str, run: Run):
    await conn.fetchrow(f'SELECT id FROM {schema}.run WHERE id=$1 FOR UPDATE',
                        run.run_id)


async def db_instance_upsert(conn, schema: str, instance: Instance):
    ins_id = await conn.fetchrow(
        f'INSERT INTO {schema}.instance \
            (run_id, run_date, filename, boundary, flag_log, reliability_plot,\
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
            parameters=EXCLUDED.parameters,  \
            version=EXCLUDED.version,  \
            return_code=EXCLUDED.return_code,  \
            stdout=EXCLUDED.stdout,  \
            stderr=EXCLUDED.stderr  \
        RETURNING id',
        instance.run_id,
        instance.run_date,
        instance.filename,
        instance.boundary,
        instance.flag_log,
        instance.reliability_plot,
        instance.log,
        json.dumps(instance.params),
        instance.version,
        instance.return_code,
        instance.stdout,
        instance.stderr
    )
    instance.instance_id = ins_id[0]
    return instance


async def db_source_match(conn, schema: str, run_id: int,
                          detection: dict, uncertainty_sigma: int):
    x = detection['x']
    y = detection['y']
    z = detection['z']
    err_x = detection['err_x']
    err_y = detection['err_y']
    err_z = detection['err_z']

    result = await conn.fetch(
        f"""SELECT
            d.id, d.instance_id, x, y, z, f_sum, ell_maj,
            ell_min, w50, w20, flag, unresolved
            FROM {schema}.detection as d, {schema}.instance as i
            WHERE
            ST_3DDistance(geometry(ST_MakePoint($1, $2, 0)), geometry(ST_MakePoint(x, y, 0)))
            <= {uncertainty_sigma} * SQRT( (($1 - x)^2 * ($4^2 + err_x^2) + ($2 - y)^2 * ($5^2 + err_y^2))
            / COALESCE( NULLIF( (($1 - x)^2 + ($2 - y)^2), 0), 1) )
            AND
            ST_3DDistance( geometry(ST_MakePoint(0, 0, $3)), geometry(ST_MakePoint(0, 0, z)))
            <= {uncertainty_sigma} * SQRT($6^2 + err_z^2)
            AND d.instance_id = i.id
            AND i.run_id = $7
            ORDER BY d.id
            ASC FOR UPDATE OF d""",
        x,
        y,
        z,
        err_x,
        err_y,
        err_z,
        run_id)

    for i, j in enumerate(result):
        # do not want the original detection if it already exists
        if j['x'] == x and j['y'] == y and j['z'] == z:
            result.pop(i)
            break
    return result


def _check_bytea(var):
    num_bytes = sys.getsizeof(var)
    if num_bytes < MAX_BYTEA:
        return var, num_bytes
    else:
        return None, 0


async def db_detection_product_insert(conn, schema, detection_id, cube, mask,
                                      mom0, mom1, mom2, chan, spec, pv):

    cube_bytes, cube_bytes_t = _check_bytea(cube)
    if cube_bytes is None:
        logging.warn(f"cube for {detection_id} too large, ignoring")

    mask_bytes, mask_bytes_t = _check_bytea(mask)
    if mask_bytes is None:
        logging.warn(f"mask for {detection_id} too large, ignoring")

    mom0_bytes, mom0_bytes_t = _check_bytea(mom0)
    if mom0_bytes is None:
        logging.warn(f"mom0 for {detection_id} too large, ignoring")

    mom1_bytes, mom1_bytes_t = _check_bytea(mom1)
    if mom1_bytes is None:
        logging.warn(f"mom1 for {detection_id} too large, ignoring")

    mom2_bytes, mom2_bytes_t = _check_bytea(mom2)
    if mom2_bytes is None:
        logging.warn(f"mom2 for {detection_id} too large, ignoring")

    chan_bytes, chan_bytes_t = _check_bytea(chan)
    if chan_bytes is None:
        logging.warn(f"chan for {detection_id} too large, ignoring")

    spec_bytes, spec_bytes_t = _check_bytea(spec)
    if spec_bytes is None:
        logging.warn(f"spec for {detection_id} too large, ignoring")

    pv_bytes, pv_bytes_t = _check_bytea(pv)
    if pv_bytes is None:
        logging.warn(f"pv for {detection_id} too large, ignoring")

    total_bytes = cube_bytes_t + mask_bytes_t + mom0_bytes_t + mom1_bytes_t + chan_bytes_t + spec_bytes_t + pv_bytes_t
    if total_bytes > MAX_BYTEA:
        total_bytes = mom0_bytes_t + mom1_bytes_t + spec_bytes_t
        if total_bytes < MAX_BYTEA:
            cube_bytes = None
            mask_bytes = None
            chan_bytes = None
        else:
            logging.warn(f"Products for {detection_id} too large, ignoring")
            return

    product_id = await conn.fetchrow(
        f'INSERT INTO {schema}.product \
            (detection_id, cube, mask, mom0, \
            mom1, mom2, chan, spec, pv) \
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9) \
        ON CONFLICT (detection_id) \
        DO UPDATE SET detection_id=EXCLUDED.detection_id \
        RETURNING id',
        detection_id,
        cube_bytes,
        mask_bytes,
        mom0_bytes,
        mom1_bytes,
        mom2_bytes,
        chan_bytes,
        spec_bytes,
        pv_bytes)


async def db_detection_insert(conn, schema: str, vo_datalink_url: str, run_id: int, instance_id: int,
                              detection: dict, cube: bytes, mask: bytes,
                              mom0: bytes, mom1: bytes, mom2: bytes,
                              chan: bytes, spec: bytes, pv: bytes,
                              unresolved: bool = False):

    detection['run_id'] = run_id
    detection['instance_id'] = instance_id
    detection['unresolved'] = unresolved

    for _, key in enumerate(Const.FULL_SCHEMA):
        if detection.get(key, None) is None:
            detection[key] = Const.FULL_SCHEMA[key]

    detection_id = await conn.fetchrow(
        f'INSERT INTO {schema}.detection \
            (run_id, instance_id, unresolved, name, x, y, z, x_min, x_max, \
            y_min, y_max, z_min, z_max, n_pix, f_min, f_max, f_sum, rel, \
            flag, rms, w20, w50, ell_maj, ell_min, ell_pa, ell3s_maj, \
            ell3s_min, ell3s_pa, kin_pa, err_x, err_y, err_z, err_f_sum, \
            ra, dec, freq, l, b, v_rad, v_opt, v_app, \
            wm50, x_peak, y_peak, z_peak, ra_peak, dec_peak, \
            freq_peak, l_peak, b_peak, v_rad_peak, v_opt_peak, v_app_peak, \
            access_url) \
        VALUES(\
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,\
            $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,\
            $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, \
            $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, \
            $54 || currval(pg_get_serial_sequence(\'{schema}.detection\', \'id\'))) \
        ON CONFLICT (\
            name, x, y, z, x_min, x_max, y_min, y_max, z_min, z_max, \
            n_pix, f_min, f_max, f_sum, instance_id, run_id) \
        DO UPDATE SET ra=EXCLUDED.ra, unresolved=EXCLUDED.unresolved \
        RETURNING id',
        detection['run_id'], detection['instance_id'], detection['unresolved'],
        detection['name'], detection['x'], detection['y'], detection['z'],
        detection['x_min'], detection['x_max'],
        detection['y_min'], detection['y_max'], detection['z_min'],
        detection['z_max'], detection['n_pix'], detection['f_min'],
        detection['f_max'], detection['f_sum'],
        detection['rel'], detection['flag'], detection['rms'],
        detection['w20'], detection['w50'], detection['ell_maj'],
        detection['ell_min'], detection['ell_pa'],
        detection['ell3s_maj'], detection['ell3s_min'],
        detection['ell3s_pa'], detection['kin_pa'], detection['err_x'],
        detection['err_y'], detection['err_z'], detection['err_f_sum'],
        detection['ra'], detection['dec'], detection['freq'], detection['l'],
        detection['b'], detection['v_rad'], detection['v_opt'],
        detection['v_app'], detection['wm50'],
        detection['x_peak'], detection['y_peak'], detection['z_peak'],
        detection['ra_peak'], detection['dec_peak'], detection['freq_peak'],
        detection['l_peak'], detection['b_peak'], detection['v_rad_peak'],
        detection['v_opt_peak'], detection['v_app_peak'],
        vo_datalink_url
    )

    await db_detection_product_insert(conn, schema, detection_id[0], cube, mask, mom0,
                                      mom1, mom2, chan, spec, pv)
    return detection_id[0]


async def db_delete_detection(conn, schema: str, detection_id: int):
    await conn.fetchrow(
        f'DELETE FROM {schema}.detection WHERE id=$1',
        detection_id
    )


async def db_update_detection_unresolved(conn, schema: str, unresolved: bool,
                                         detection_id_list: list):
    await conn.fetchrow(
        f'UPDATE {schema}.detection \
        SET unresolved=$1 \
        WHERE id = ANY($2::bigint[])',
        unresolved,
        detection_id_list
    )
