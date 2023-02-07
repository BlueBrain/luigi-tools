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

"""Tests for luigi-tools targets."""

# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
import os
import re
import shutil
from pathlib import Path

import luigi
import pytest

import luigi_tools.target
import luigi_tools.task
import luigi_tools.util

from .tools import create_not_empty_file


@pytest.fixture
def reset_prefix():
    """Fixture to reset the prefix of the `luigi_tools.target.OutputLocalTarget` class."""
    assert luigi_tools.target.OutputLocalTarget.get_default_prefix() == Path()
    yield
    luigi_tools.target.OutputLocalTarget.set_default_prefix(None)


class TestOutputTarget:
    """Test the `luigi_tools.target.OutputLocalTarget` class."""

    def test_get_default_prefix(self, tmpdir, reset_prefix):
        """Test the `get_default_prefix` class method."""
        expected = Path()
        assert luigi_tools.target.OutputLocalTarget.get_default_prefix() == expected
        assert (
            luigi_tools.target.OutputLocalTarget("path", create_parent=False).get_default_prefix()
            == expected
        )
        assert (
            luigi_tools.target.OutputLocalTarget(
                "path", prefix=tmpdir, create_parent=False
            ).get_default_prefix()
            == expected
        )

        luigi_tools.target.OutputLocalTarget.set_default_prefix(tmpdir / "subdir")

        expected = Path(tmpdir / "subdir")
        assert luigi_tools.target.OutputLocalTarget.get_default_prefix() == expected
        assert (
            luigi_tools.target.OutputLocalTarget("path", create_parent=False).get_default_prefix()
            == expected
        )
        assert (
            luigi_tools.target.OutputLocalTarget(
                "path", prefix=tmpdir, create_parent=False
            ).get_default_prefix()
            == expected
        )

    def test_prefix(self, tmpdir, reset_prefix):
        """Test the `get_prefix` method."""
        assert (
            luigi_tools.target.OutputLocalTarget("path", create_parent=False).get_prefix() == Path()
        )
        assert luigi_tools.target.OutputLocalTarget(
            "path", prefix=tmpdir, create_parent=False
        ).get_prefix() == Path(tmpdir)

        luigi_tools.target.OutputLocalTarget.set_default_prefix(tmpdir / "subdir")

        assert luigi_tools.target.OutputLocalTarget(
            "path", create_parent=False
        ).get_prefix() == Path(tmpdir / "subdir")
        assert (
            luigi_tools.target.OutputLocalTarget(
                "path", prefix=tmpdir / "test_subdir", create_parent=False
            ).get_prefix()
            == Path(tmpdir) / "test_subdir"
        )
        assert luigi_tools.target.OutputLocalTarget(
            "path", prefix="test_subdir", create_parent=False
        ).get_prefix() == Path("test_subdir")

    def test_pathlib_path(self, tmpdir, reset_prefix):
        """Test the `pathlib_path` property."""
        tmpdir = Path(tmpdir)

        # No default prefix and no prefix
        assert luigi_tools.target.OutputLocalTarget(
            "path", create_parent=False
        ).pathlib_path == Path("path")

        # No default prefix and absolute prefix
        assert (
            luigi_tools.target.OutputLocalTarget(
                "path", prefix=tmpdir, create_parent=False
            ).pathlib_path
            == tmpdir / "path"
        )

        # No default prefix and relative prefix
        assert (
            luigi_tools.target.OutputLocalTarget(
                "path", prefix="test_subdir", create_parent=False
            ).pathlib_path
            == Path("test_subdir") / "path"
        )

        # Set absolute default prefix
        luigi_tools.target.OutputLocalTarget.set_default_prefix(tmpdir / "absolute_default_subdir")

        # Default prefix and no prefix
        assert (
            luigi_tools.target.OutputLocalTarget("path", create_parent=False).pathlib_path
            == tmpdir / "absolute_default_subdir" / "path"
        )

        # Default prefix and absolute prefix
        assert (
            luigi_tools.target.OutputLocalTarget(
                "path", prefix=tmpdir / "other_subdir", create_parent=False
            ).pathlib_path
            == tmpdir / "other_subdir" / "path"
        )

        # Default prefix and relative prefix (replace only the default suffix of the current class)
        assert (
            luigi_tools.target.OutputLocalTarget(
                "path", prefix="test_subdir", create_parent=False
            ).pathlib_path
            == Path("test_subdir") / "path"
        )

        # Set relative default prefix
        luigi_tools.target.OutputLocalTarget.set_default_prefix("relative_default_subdir")

        # Default prefix and no prefix
        assert (
            luigi_tools.target.OutputLocalTarget("path", create_parent=False).pathlib_path
            == Path("relative_default_subdir") / "path"
        )

        # Default prefix and absolute prefix
        assert (
            luigi_tools.target.OutputLocalTarget(
                "path", prefix=tmpdir / "other_subdir", create_parent=False
            ).pathlib_path
            == tmpdir / "other_subdir" / "path"
        )

        # Default prefix and relative prefix (replace only the default suffix of the current class)
        assert (
            luigi_tools.target.OutputLocalTarget(
                "path", prefix="test_subdir", create_parent=False
            ).pathlib_path
            == Path("test_subdir") / "path"
        )

    def test_in_task(self, tmpdir, reset_prefix):
        """Test the `luigi_tools.target.OutputLocalTarget` class used in an actual `luigi.task`.

        Several tests for the OutputLocalTarget class:

        * using explicit prefix, so the default prefix is ignored
        * using absolute path, so the prefix is ignored
        * using explicit prefix with relative path, so the default prefix is ignored
        * using explicit prefix with absolute path, so the prefix is ignored
        * using default prefix
        """

        class TaskA_OutputLocalTarget(luigi_tools.task.WorkflowTask):
            """A simple test task."""

            def run(self):
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
                    luigi_tools.target.OutputLocalTarget(
                        "output_target.test", prefix=tmpdir, create_parent=False
                    ),
                    luigi_tools.target.OutputLocalTarget(
                        tmpdir / "absolute_output_target_no_prefix.test",
                        create_parent=False,
                    ),
                    luigi_tools.target.OutputLocalTarget(
                        "relative_output_target.test",
                        prefix=tmpdir / "test" / "..",
                        create_parent=False,
                    ),
                    luigi_tools.target.OutputLocalTarget(
                        "output_target_default_prefix.test", create_parent=False
                    ),
                    luigi_tools.target.OutputLocalTarget(
                        tmpdir / "absolute_output_target_prefix.test",
                        prefix=tmpdir / "test",
                        create_parent=False,
                    ),
                ]

        luigi_tools.target.OutputLocalTarget.set_default_prefix(tmpdir / "subdir")

        assert luigi.build([TaskA_OutputLocalTarget()], local_scheduler=True)

        luigi_tools.target.OutputLocalTarget.set_default_prefix(None)
        target = luigi_tools.target.OutputLocalTarget("test", prefix=None, create_parent=False)
        assert target.path == "test"
        luigi_tools.target.OutputLocalTarget.set_default_prefix(None)
        assert luigi_tools.target.OutputLocalTarget.get_default_prefix().absolute() == Path(
            os.getcwd()
        )

        other_subdir = tmpdir / "other_subdir"
        luigi_tools.target.OutputLocalTarget.set_default_prefix(other_subdir / "test" / "create")
        target = luigi_tools.target.OutputLocalTarget("test_file", prefix=None, create_parent=False)

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

        class NoPrefix(luigi_tools.target.OutputLocalTarget):
            """A target with no prefix."""

        assert NoPrefix("path").path == other_subdir / "test" / "create" / "path"

    def test_super_prefix(self, tmpdir, reset_prefix):
        """Test the super prefix to build a target hierarchy."""

        # pylint: disable=useless-super-delegation
        class SubOutputLocalTarget(luigi_tools.target.OutputLocalTarget):
            """A simple target with a `str` prefix."""

            __prefix = "sub_prefix"  # Use a string

            def __init__(self, *args, prefix=None, create_parent=False, **kwargs):
                super().__init__(*args, prefix=prefix, create_parent=create_parent, **kwargs)

        class SubSubOutputLocalTarget(SubOutputLocalTarget):
            """A simple target with a `pathlib.Path` prefix."""

            __prefix = Path("sub_sub_prefix")  # Use a pathlib.Path

            def __init__(self, *args, prefix=None, create_parent=False, **kwargs):
                super().__init__(*args, prefix=prefix, create_parent=create_parent, **kwargs)

        class NoPrefix(SubSubOutputLocalTarget):
            """Test that a target without prefix in the MRO does not break the feature."""

            def __init__(self, *args, prefix=None, create_parent=False, **kwargs):
                super().__init__(*args, prefix=prefix, create_parent=create_parent, **kwargs)

        class SubSubSubOutputLocalTarget(NoPrefix):
            """A simple target with another sub-prefix."""

            __prefix = Path("sub_sub_sub_prefix")

            def __init__(self, *args, prefix=None, create_parent=False, **kwargs):
                super().__init__(*args, prefix=prefix, create_parent=create_parent, **kwargs)

        assert luigi_tools.target.OutputLocalTarget("path", create_parent=False).path == "path"

        # Test with default prefixes
        assert SubOutputLocalTarget("path").path == "sub_prefix/path"
        assert SubSubOutputLocalTarget("path").path == "sub_prefix/sub_sub_prefix/path"
        assert (
            SubSubSubOutputLocalTarget("path").path
            == "sub_prefix/sub_sub_prefix/sub_sub_sub_prefix/path"
        )

        # Test with new relative path for parent
        SubOutputLocalTarget.set_default_prefix("new_sub_prefix")
        assert SubOutputLocalTarget("path").path == "new_sub_prefix/path"
        assert SubSubOutputLocalTarget("path").path == "new_sub_prefix/sub_sub_prefix/path"
        assert (
            SubSubSubOutputLocalTarget("path").path
            == "new_sub_prefix/sub_sub_prefix/sub_sub_sub_prefix/path"
        )

        # Test with new absolute path for parent
        SubOutputLocalTarget.set_default_prefix("/tmp/sub_prefix")
        assert SubOutputLocalTarget("path").path == "/tmp/sub_prefix/path"
        assert SubSubOutputLocalTarget("path").path == "/tmp/sub_prefix/sub_sub_prefix/path"
        assert (
            SubSubSubOutputLocalTarget("path").path
            == "/tmp/sub_prefix/sub_sub_prefix/sub_sub_sub_prefix/path"
        )

        # Test with relative paths for both classes
        SubOutputLocalTarget.set_default_prefix("sub_prefix_2")
        SubSubOutputLocalTarget.set_default_prefix("sub_sub_prefix_2")
        assert SubOutputLocalTarget("path").path == "sub_prefix_2/path"
        assert SubSubOutputLocalTarget("path").path == "sub_prefix_2/sub_sub_prefix_2/path"
        assert (
            SubSubSubOutputLocalTarget("path").path
            == "sub_prefix_2/sub_sub_prefix_2/sub_sub_sub_prefix/path"
        )

        # Test with absolute paths for parent class and relative path for child
        SubOutputLocalTarget.set_default_prefix("/tmp/sub_prefix_2")
        SubSubOutputLocalTarget.set_default_prefix("sub_sub_prefix_2")
        assert SubOutputLocalTarget("path").path == "/tmp/sub_prefix_2/path"
        assert SubSubOutputLocalTarget("path").path == "/tmp/sub_prefix_2/sub_sub_prefix_2/path"
        assert (
            SubSubSubOutputLocalTarget("path").path
            == "/tmp/sub_prefix_2/sub_sub_prefix_2/sub_sub_sub_prefix/path"
        )

        # Test with absolute paths for both parent and child classes
        SubOutputLocalTarget.set_default_prefix("/tmp/sub_prefix_2")
        SubSubOutputLocalTarget.set_default_prefix("/tmp/sub_sub_prefix_2")
        assert SubOutputLocalTarget("path").path == "/tmp/sub_prefix_2/path"
        assert SubSubOutputLocalTarget("path").path == "/tmp/sub_sub_prefix_2/path"
        assert (
            SubSubSubOutputLocalTarget("path").path
            == "/tmp/sub_sub_prefix_2/sub_sub_sub_prefix/path"
        )

        # Reset prefix
        SubSubOutputLocalTarget.set_default_prefix(None)
        assert SubOutputLocalTarget("path").path == "/tmp/sub_prefix_2/path"
        assert SubSubOutputLocalTarget("path").path == "/tmp/sub_prefix_2/path"
        assert (
            SubSubSubOutputLocalTarget("path").path == "/tmp/sub_prefix_2/sub_sub_sub_prefix/path"
        )

        # Reset parent prefix
        SubOutputLocalTarget.set_default_prefix(None)
        assert SubOutputLocalTarget("path").path == "path"
        assert SubSubOutputLocalTarget("path").path == "path"
        assert SubSubSubOutputLocalTarget("path").path == "sub_sub_sub_prefix/path"

    @pytest.fixture
    def reset_target_no_prefix(self):
        """Fixture to reset the prefix of the `luigi_tools.target.OutputLocalTarget` class."""
        luigi_tools.target.OutputLocalTarget.set_default_prefix(None)
        yield
        luigi_tools.target.OutputLocalTarget.set_default_prefix(None)

    def test_child_classes(self, reset_target_no_prefix):
        """Test a child class without any default prefix."""

        class TestTarget(luigi_tools.target.OutputLocalTarget):
            """A simple test task."""

            def __init__(self, *args, prefix=None, create_parent=False, **kwargs):
                super().__init__(*args, prefix=prefix, create_parent=create_parent, **kwargs)

        test_target = TestTarget("a_path")
        assert test_target.path == "a_path"

        test_target = TestTarget("a_path", prefix="instance_prefix")
        assert test_target.path == "instance_prefix/a_path"

    @pytest.fixture
    def reset_target_relative_prefix(self):
        """Fixture to reset the prefix of the `luigi_tools.target.OutputLocalTarget` class."""
        luigi_tools.target.OutputLocalTarget.set_default_prefix("parent_default_prefix")
        yield
        luigi_tools.target.OutputLocalTarget.set_default_prefix(None)

    def test_child_classes_with_parent_prefix(self, reset_target_relative_prefix):
        """Test a child class when a default prefix is given to OutputLocalTarget."""
        # pylint: disable=useless-super-delegation

        class TestTarget(luigi_tools.target.OutputLocalTarget):
            """A simple test task."""

            def __init__(self, *args, prefix=None, create_parent=False, **kwargs):
                super().__init__(*args, prefix=prefix, create_parent=create_parent, **kwargs)

        class TestTargetChild(TestTarget):
            """A simple test task."""

            def __init__(self, *args, prefix=None, create_parent=False, **kwargs):
                super().__init__(*args, prefix=prefix, create_parent=create_parent, **kwargs)

        test_target = TestTarget("a_path")
        assert test_target.path == "parent_default_prefix/a_path"

        test_target = TestTarget("a_path", prefix="instance_prefix")
        assert test_target.path == "parent_default_prefix/instance_prefix/a_path"

        # Add a default prefix to TestTarget
        TestTarget.set_default_prefix("default_prefix")

        test_target = TestTarget("a_path")
        assert test_target.path == "parent_default_prefix/default_prefix/a_path"

        test_target = TestTarget("a_path", prefix="instance_prefix")
        assert test_target.path == "parent_default_prefix/instance_prefix/a_path"

        # Add a default prefix to TestTargetChild
        test_target = TestTargetChild("a_path")
        assert test_target.path == "parent_default_prefix/default_prefix/a_path"

        test_target = TestTargetChild("a_path", prefix="child_instance_prefix")
        assert (
            test_target.path == "parent_default_prefix/default_prefix/child_instance_prefix/a_path"
        )

        TestTargetChild.set_default_prefix("child_default_prefix")

        test_target = TestTargetChild("a_path")
        assert (
            test_target.path == "parent_default_prefix/default_prefix/child_default_prefix/a_path"
        )

        test_target = TestTargetChild("a_path", prefix="child_instance_prefix")
        assert (
            test_target.path == "parent_default_prefix/default_prefix/child_instance_prefix/a_path"
        )

        # Remove the default prefix of TestTarget
        TestTarget.set_default_prefix(None)

        test_target = TestTargetChild("a_path")
        assert test_target.path == "parent_default_prefix/child_default_prefix/a_path"

        test_target = TestTargetChild("a_path", prefix="child_instance_prefix")
        assert test_target.path == "parent_default_prefix/child_instance_prefix/a_path"

        # Remove the default prefix of TestTargetChild
        TestTargetChild.set_default_prefix(None)

        test_target = TestTargetChild("a_path")
        assert test_target.path == "parent_default_prefix/a_path"

        test_target = TestTargetChild("a_path", prefix="child_instance_prefix")
        assert test_target.path == "parent_default_prefix/child_instance_prefix/a_path"

    def test_repr_and_str(self):
        """Test the __repr__ and __str__ methods."""
        assert str(luigi_tools.target.OutputLocalTarget("path", create_parent=False)) == "path"
        assert (
            str(
                luigi_tools.target.OutputLocalTarget(
                    "path", prefix="test_prefix", create_parent=False
                )
            )
            == "test_prefix/path"
        )

        assert (
            re.match(
                r"<OutputLocalTarget at 0x\S+; path>",
                repr(luigi_tools.target.OutputLocalTarget("path", create_parent=False)),
            )
            is not None
        )
        assert (
            re.match(
                r"<OutputLocalTarget at 0x\S+; test_prefix/path>",
                repr(
                    luigi_tools.target.OutputLocalTarget(
                        "path", prefix="test_prefix", create_parent=False
                    )
                ),
            )
            is not None
        )

        try:
            luigi_tools.target.OutputLocalTarget.set_default_prefix("default_prefix")

            assert (
                re.match(
                    r"<OutputLocalTarget at 0x\S+; default_prefix/path>",
                    repr(luigi_tools.target.OutputLocalTarget("path", create_parent=False)),
                )
                is not None
            )
            assert (
                re.match(
                    r"<OutputLocalTarget at 0x\S+; test_prefix/path>",
                    repr(
                        luigi_tools.target.OutputLocalTarget(
                            "path", prefix="test_prefix", create_parent=False
                        )
                    ),
                )
                is not None
            )
        finally:
            luigi_tools.target.OutputLocalTarget.set_default_prefix(None)
