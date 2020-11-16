"""This module provides some specific luigi targets."""
import os
from pathlib import Path

import luigi


class OutputLocalTarget(luigi.LocalTarget):
    """A target that adds a prefix before the given path.

    If prefix is not given, the current working directory is taken.
    """

    _prefix = None

    def __init__(self, *args, prefix=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._reset_prefix(self, prefix)

    @property
    def path(self):
        """The path stored in this target."""
        return str(self.pathlib_path)

    @path.setter
    def path(self, path):
        self._path = Path(path)

    @property
    def pathlib_path(self):
        """The path stored in this target returned as a ``pathlib.Path`` object."""
        if self._prefix is not None:
            return self._prefix / self._path
        else:
            return self._path

    @classmethod
    def set_default_prefix(cls, prefix):
        """Set the default prefix to the class.

        .. warning:: This method is not thread-safe.
        """
        OutputLocalTarget._reset_prefix(cls, prefix)

    @staticmethod
    def _reset_prefix(obj, prefix):
        # pylint: disable=protected-access
        if prefix is not None:
            obj._prefix = Path(prefix).absolute()
        elif obj._prefix is None:
            obj._prefix = Path(os.getcwd())
        else:
            obj._prefix = Path(obj._prefix)
