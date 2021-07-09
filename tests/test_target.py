"""Tests for luigi-tools targets.

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
import shutil
from pathlib import Path

import luigi
import pytest

import luigi_tools.task
import luigi_tools.target
import luigi_tools.util

from .tools import create_not_empty_file


def test_output_target(tmpdir):
    """
    4 tests for the OutputLocalTarget class:
        * using explicit prefix, so the default prefix is ignored
        * using absolute path, so the prefix is ignored
        * using explicit prefix with relative paths, so the default prefix is ignored
        * using default prefix
    """

    class TaskA_OutputLocalTarget(luigi_tools.task.WorkflowTask):
        """"""

        def run(self):
            """"""
            expected = [
                str(tmpdir / "output_target.test"),
                str(tmpdir / "absolute_output_target_no_prefix.test"),
                str(tmpdir / "relative_output_target.test"),
                str(tmpdir / "subdir" / "output_target_default_prefix.test"),
                str(tmpdir / "absolute_output_target_prefix.test"),
            ]

            assert [i.path for i in luigi.task.flatten(self.output())] == expected

            for i in luigi.task.flatten(self.output()):
                os.makedirs(i.pathlib_path.parent, exist_ok=True)
                create_not_empty_file(i.path)
                assert i.exists()
                luigi_tools.util.target_remove(i)
                assert not i.exists()

        def output(self):
            return [
                luigi_tools.target.OutputLocalTarget("output_target.test", prefix=tmpdir),
                luigi_tools.target.OutputLocalTarget(
                    tmpdir / "absolute_output_target_no_prefix.test"
                ),
                luigi_tools.target.OutputLocalTarget(
                    "relative_output_target.test", prefix=tmpdir / "test" / ".."
                ),
                luigi_tools.target.OutputLocalTarget("output_target_default_prefix.test"),
                luigi_tools.target.OutputLocalTarget(
                    tmpdir / "absolute_output_target_prefix.test",
                    prefix=tmpdir / "test",
                ),
            ]

    current_prefix = luigi_tools.target.OutputLocalTarget._prefix

    try:
        current_dir = Path(os.getcwd())
        assert luigi_tools.target.OutputLocalTarget("path").get_default_prefix() == current_dir
        assert luigi_tools.target.OutputLocalTarget(
            "path", prefix=tmpdir
        ).get_default_prefix() == Path(tmpdir)
        luigi_tools.target.OutputLocalTarget.set_default_prefix(tmpdir / "subdir")
        assert luigi.build([TaskA_OutputLocalTarget()], local_scheduler=True)
    finally:
        luigi_tools.target.OutputLocalTarget.set_default_prefix(current_prefix)

    try:
        luigi_tools.target.OutputLocalTarget._prefix = None
        target = luigi_tools.target.OutputLocalTarget("test", prefix=None)
        target._prefix = None
        assert target.path == "test"
        luigi_tools.target.OutputLocalTarget.set_default_prefix(None)
        assert luigi_tools.target.OutputLocalTarget._prefix.as_posix() == os.getcwd()
    finally:
        luigi_tools.target.OutputLocalTarget.set_default_prefix(current_prefix)

    try:
        other_subdir = tmpdir / "other_subdir"
        luigi_tools.target.OutputLocalTarget.set_default_prefix(other_subdir / "test" / "create")
        target = luigi_tools.target.OutputLocalTarget("test_file", prefix=None)

        target.mkdir()
        assert target.pathlib_path.parent.is_dir()
        assert not target.pathlib_path.exists()
        shutil.rmtree(other_subdir)

        target.mkdir(is_dir=True)
        assert target.pathlib_path.is_dir()

        with pytest.raises(OSError):
            target.mkdir(exist_ok=False)

        shutil.rmtree(other_subdir)
        with pytest.raises(FileNotFoundError):
            target.mkdir(parents=False)

        auto_target = luigi_tools.target.OutputLocalTarget("test_file", create_parent=True)
        assert auto_target.pathlib_path.parent.is_dir()
        assert not auto_target.pathlib_path.exists()

        not_auto_target = luigi_tools.target.OutputLocalTarget(
            "test_dir/test_file", create_parent=False
        )
        assert not not_auto_target.pathlib_path.parent.exists()
        assert not not_auto_target.pathlib_path.exists()
    finally:
        luigi_tools.target.OutputLocalTarget.set_default_prefix(current_prefix)
