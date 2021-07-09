"""This module provides some specific luigi targets.

Copyright 2021 Blue Brain Project / EPFL

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import os
from pathlib import Path

import luigi


class OutputLocalTarget(luigi.LocalTarget):
    """A target that adds a prefix before the given path.

    Args:
        prefix (str): The prefix to use. If not given, the current working directory is taken.
        create_parent (bool): If set to ``True``, the parent directory is automatically
            created.

    This class can be subclassed to easily create an output directory tree.

    .. note::
        If an absolute path is given to the target, the prefix is ignored.

    **Usage**:

    .. code-block:: python

        class PathConfig(luigi.Config):
            '''Paths config.'''
            result_path = luigi.Parameter()
            result_sub_path_1 = luigi.Parameter()
            result_sub_path_2 = luigi.Parameter()

        class Sub1OutputLocalTarget(OutputLocalTarget):
            '''Specific target for first category outputs.'''

        class Sub2OutputLocalTarget(OutputLocalTarget):
            '''Specific target for second category outputs.'''

        OutputLocalTarget.set_default_prefix(PathConfig().result_path)
        Sub1OutputLocalTarget.set_default_prefix(
            OutputLocalTarget._prefix / PathConfig().result_sub_path_1
        )
        Sub2OutputLocalTarget.set_default_prefix(
            OutputLocalTarget._prefix / PathConfig().result_sub_path_2
        )

        class TaskA(luigi.Task):
            def run(self):
                # do something
                # and write output
                write_output(self.output().path)

            def output(self):
                return Sub1OutputLocalTarget("file1.dat")

        class TaskB(luigi.Task):
            def run(self):
                # do something
                # and write outputs
                f1, f2, f3 = self.output()
                write_output1(f1.path)
                write_output2(f2.path)
                write_output3(f3.path)

            def output(self):
                return [
                    Sub1OutputLocalTarget("file2.dat"),
                    Sub2OutputLocalTarget("file1.dat"),
                    Sub2OutputLocalTarget("file2.dat"),
                ]

    Running this luigi workflow creates the following output directory tree:

    .. code-block:: bash

        └── result_path
            ├── sub_path_1
            │   ├── file1.dat
            │   └── file2.dat
            └── sub_path_2
                ├── file1.dat
                └── file2.dat
    """

    _prefix = None

    def __init__(self, *args, prefix=None, create_parent=True, **kwargs):
        super().__init__(*args, **kwargs)
        self._reset_prefix(self, prefix)
        if create_parent:
            self.mkdir()

    @property
    def path(self):
        """The path stored in this target."""
        return str(self.pathlib_path)

    @path.setter
    def path(self, path):
        self._path = Path(path)

    @property
    def pathlib_path(self):
        """The path stored in this target returned as a :class:`pathlib.Path` object."""
        if self._prefix is not None:
            return self._prefix / self._path
        else:
            return self._path

    @classmethod
    def set_default_prefix(cls, prefix):
        """Set the default prefix to the class.

        .. warning::
            This method is not thread-safe and should not be used inside a
            :class:`luigi.Task`.
        """
        OutputLocalTarget._reset_prefix(cls, prefix)

    def get_default_prefix(self):
        """Return the default prefix."""
        return self._prefix

    @staticmethod
    def _reset_prefix(obj, prefix):
        # pylint: disable=protected-access
        if prefix is not None:
            obj._prefix = Path(prefix).absolute()
        elif obj._prefix is None:
            obj._prefix = Path(os.getcwd())
        else:
            obj._prefix = Path(obj._prefix)

    def mkdir(self, is_dir=False, mode=511, parents=True, exist_ok=True):
        """Create the directory of this path.

        Args:
            is_dir (bool): if set to True, the current path is create, otherwise only the parent
                directory is create.
            mode (int): numeric mode used to create the directory with given permissions (unix
                only).
            parents (bool): if set to True, the parents are also created if they are missing.
            exist_ok (bool): if set to True, do not raise an exception if the directory already
                exists.
        """
        if is_dir:
            self.pathlib_path.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
        else:
            self.pathlib_path.parent.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
