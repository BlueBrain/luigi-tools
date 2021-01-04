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


class set_luigi_config:
    """Context manager to set current luigi config."""

    def __init__(self, params=None):
        self.params = params
        self.luigi_config = luigi.configuration.get_config()

    def __enter__(self):
        self.configfile = "luigi.cfg"

        # Reset luigi config
        self.luigi_config.clear()

        # Remove config file
        if os.path.exists(self.configfile):
            os.remove(self.configfile)

        # Export config
        if self.params is not None:
            export_config(self.params, self.configfile)

            # Set current config in luigi
            self.luigi_config.read(self.configfile)
        else:
            self.configfile = None

    def __exit__(self, *args):
        # Remove config file
        if self.configfile is not None and os.path.exists(self.configfile):
            os.remove(self.configfile)

        # Reset luigi config
        self.luigi_config.clear()
