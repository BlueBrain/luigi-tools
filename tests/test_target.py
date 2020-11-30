"""Tests for luigi tools."""
import os

import luigi

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

    try:
        current_prefix = luigi_tools.target.OutputLocalTarget._prefix
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
