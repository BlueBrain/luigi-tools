# Copyright 2021 Blue Brain Project / EPFL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This module provides some specific luigi targets."""
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

        class Sub1OutputLocalTarget(OutputLocalTarget):
            '''Specific target for first category outputs.'''
            __prefix = "sub_path_1"

        class Sub2OutputLocalTarget(Sub1OutputLocalTarget):
            '''Specific target for second category outputs.'''
            __prefix = "sub_path_2"

        OutputLocalTarget.set_default_prefix(PathConfig().result_path)

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
            └── sub_path_1
                ├── file1.dat
                └── file2.dat
                └── sub_path_2
                    ├── file1.dat
                    └── file2.dat
    """

    __prefix = Path()  # pylint: disable=unused-private-member

    def __init__(self, *args, prefix=None, create_parent=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_prefix(prefix)
        if create_parent:
            self.mkdir()

    @classmethod
    def _mangled_prefix_name(cls):
        attr_name = "_" + cls.__name__ + "__prefix"
        return attr_name

    def __repr__(self):
        """Custom repr method to include the path."""
        return f"<{self.__class__.__name__} at 0x{id(self)}; {self.path}>"

    def __str__(self):
        """Custom str method to return the path of this target."""
        return self.path

    @property
    def path(self):
        """The path stored in this target."""
        return str(self.pathlib_path)

    @path.setter
    def path(self, path):
        self._path = Path(path)

    @classmethod
    def super_prefix(cls):
        """Build a prefix from the default prefixes of the parent classes."""
        prefixes = []
        for base in cls.__mro__[1:]:
            try:
                prefixes.append(base.get_default_prefix())
            except AttributeError:
                continue
        return Path(*prefixes[::-1])

    @property
    def pathlib_path(self):
        """The path stored in this target returned as a :class:`pathlib.Path` object."""
        return (self.super_prefix() / self.get_prefix()) / self._path

    @staticmethod
    def _format_prefix(prefix):
        return Path(prefix or "")

    @classmethod
    def get_default_prefix(cls):
        """Return the default prefix."""
        return cls._format_prefix(getattr(cls, cls._mangled_prefix_name(), ""))

    @classmethod
    def set_default_prefix(cls, prefix):
        """Set the default prefix to the class.

        .. warning::
            This method is not thread-safe and should not be used inside a :class:`luigi.Task`.
        """
        setattr(cls, cls._mangled_prefix_name(), cls._format_prefix(prefix))

    def get_prefix(self):
        """Return the default prefix."""
        return self._format_prefix(self._instance_prefix or self.get_default_prefix())

    def set_prefix(self, prefix):
        """Set the prefix of the current instance."""
        self._instance_prefix = prefix

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
