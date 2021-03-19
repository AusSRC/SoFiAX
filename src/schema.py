class Detection(object):
    """!Detection constants class.

    Constants for a SoFiA detection object.
    Includes SCHEMA and VO_DATALINK_URL
    """
    SCHEMA = {
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
        "flag": None
    }

    VO_DATALINK_URL = 'https://wallaby.aussrc.org/wallaby/vo/dl/dlmeta?ID='


class Run(object):
    """!Run model class.

    Represents Run table in WALLABY database. Implements methods for checking
    values of runs are reasonable.
    """
    def __init__(self, name, sanity_thresholds):
        """!Run model initialiser.

        Does some stuff you know...

        @param name                 Name of the Run.
        @param sanity_thresholds    Flux, spatial and spectral thresholds
                                    for sanity checking.
        """
        self.run_id = None
        self.name = name
        self.sanity_thresholds = sanity_thresholds
        Run.check_inputs(self.sanity_thresholds)

    @staticmethod
    def check_inputs(sanity_thresholds: dict):
        """Static method to check...

        @param sanity_thresholds    Flux, spatial and spectral thresholds
                                    for sanity checking.

        """
        # TODO(austin): refactor these try excepts
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
    """!Instance model class.

    Represents Instance table in WALLABY database. Has properties that
    mirror the schema.
    """
    def __init__(self, run_id, run_date, filename, boundary, flag_log,
                 reliability_plot, log, parameters,
                 version, return_code, stdout, stderr):
        """!Instance model initialiser.

        """
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

    def asdict(self):
        """!Convert Instance object to dictionary of member variables.

        """
        return {
            'instance_id': self.instance_id,
            'run_id': self.run_id,
            'run_date': self.run_date,
            'filename': self.filename,
            'boundary': self.boundary,
            'flag_log': self.flag_log,
            'reliability_plot': self.reliability_plot,
            'log': self.log,
            'params': self.params,
            'version': self.version,
            'return_code': self.return_code,
            'stdout': self.stdout,
            'stderr': self.stderr
        }
