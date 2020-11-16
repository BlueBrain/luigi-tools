"""A few helpers for tests"""
import os

from configparser import ConfigParser

import luigi.configuration


def create_empty_file(filename):
    """Create an empty file"""
    with open(filename, "w") as f:
        pass


def check_empty_file(filename):
    """Checck that a file is empty"""
    with open(filename) as f:
        return f.read() == ""


def create_not_empty_file(filename):
    """Create a not empty file"""
    with open(filename, "w") as f:
        f.write("NOT EMPTY")


def check_not_empty_file(filename):
    """Checck that a file is not empty"""
    with open(filename) as f:
        return f.read() == "NOT EMPTY"


def dict_to_config(params):
    config = ConfigParser()
    config.read_dict(params)
    return config


def export_config(params, filepath):
    if isinstance(params, dict):
        params = dict_to_config(params)

    # Export params
    with open(filepath, "w") as configfile:
        params.write(configfile)


def set_luigi_config(params=None):
    configfile = "luigi.cfg"

    # Reset luigi config
    luigi_config = luigi.configuration.get_config()
    luigi_config.clear()

    # Remove config file
    if os.path.exists(configfile):
        os.remove(configfile)

    # Export config
    if params is not None:
        export_config(params, configfile)

        # Set current config in luigi
        luigi_config.read(configfile)
    else:
        configfile = None

    return luigi_config, configfile
