def read_config(config, parameter):
    """Read the value of a parameter in the config file or raise an error.

    """
    param = config.get(parameter)
    if param is None:
        raise ValueError(f"{parameter} is not defined in configuration.")
    return param
