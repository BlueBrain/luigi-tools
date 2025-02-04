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

"""Tests for luigi-tools tasks."""

# pylint: disable=no-member
# pylint: disable=protected-access
# pylint: disable=too-many-lines
# pylint: disable=unused-argument
# pylint: disable=unused-variable
import importlib
import json
import logging
import warnings

import luigi
import pytest
from luigi.util import inherits
from packaging.version import Version

import luigi_tools.parameter
import luigi_tools.target
import luigi_tools.task
import luigi_tools.util
from luigi_tools.task import DuplicatedParameterError
from luigi_tools.task import GlobalParameterNoValueError
from luigi_tools.util import set_luigi_config

from .tools import check_empty_file
from .tools import check_existing_file
from .tools import check_not_empty_file
from .tools import create_empty_file
from .tools import create_not_empty_file


class TestCopyParams:
    """Test the `luigi_tools.task.copy_params` decorator."""

    @pytest.mark.filterwarnings("ignore::UserWarning:luigi.parameter")
    def test_copy_params(self, tmpdir):
        """Simple test."""

        class TaskA(luigi.Task):
            """A simple test task."""

            a = luigi.Parameter(default="default_value_a")
            b = luigi.Parameter(default="default_value_b")

            def run(self):
                print(self.a)
                return self.a

            def output(self):
                return luigi.LocalTarget(tmpdir)

        @luigi_tools.task.copy_params(
            a=luigi_tools.task.ParamRef(TaskA),
            aa=luigi_tools.task.ParamRef(TaskA, "a"),
            a_default=luigi_tools.task.ParamRef(TaskA, "a", "given_default_value"),
            a_none=luigi_tools.task.ParamRef(TaskA, "a", None),
        )
        class TaskB(luigi.Task):
            """A simple test task."""

            b = luigi.Parameter(default="b")
            b_none = luigi.Parameter(default=None)

            def run(self):
                print(self.a, self.aa, self.a_default, self.a_none, self.b, self.b_none)
                return self.a, self.aa, self.a_default, self.a_none, self.b, self.b_none

            def output(self):
                return luigi.LocalTarget(tmpdir)

        # Test with default value
        task = TaskB()
        res = task.run()

        assert res == (
            "default_value_a",
            "default_value_a",
            "given_default_value",
            None,
            "b",
            None,
        )

        # Test with another value
        task = TaskB(a="new_a", aa="new_aa", a_default="new_default", b="bb")
        res = task.run()

        assert res == ("new_a", "new_aa", "new_default", None, "bb", None)

        # Empty copy_params arguments should raise a ValueError
        with pytest.raises(ValueError):

            @luigi_tools.task.copy_params()
            class TaskC(luigi.Task):
                """A simple test task."""

                a = luigi.Parameter(default="a")

        # Duplicated parameters should raise a DuplicatedParameterError
        with pytest.raises(DuplicatedParameterError):

            @luigi_tools.task.copy_params(
                a=luigi_tools.task.ParamRef(TaskA),
            )
            class TaskD(luigi.Task):
                """A simple test task."""

                a = luigi.Parameter(default="a")

        # Test with parameters that are serialized to generate the task ID
        class TaskWithListDictParams(luigi.Task):
            """A simple test task."""

            a = luigi.ListParameter(description="a in TaskWithListDictParams")
            b = luigi.DictParameter(description="b in TaskWithListDictParams")

            def run(self):
                assert self.a == (1, 2)
                assert self.b == {"attr1": 1, "attr2": 2}

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        @luigi_tools.task.copy_params(
            a_copy=luigi_tools.task.ParamRef(TaskWithListDictParams, "a"),
            b_copy=luigi_tools.task.ParamRef(TaskWithListDictParams, "b"),
        )
        class TaskCopyListDictParams(luigi.Task):
            """A simple test task."""

            a = luigi.ListParameter(description="a in TaskCopyListDictParams")
            b = luigi.DictParameter(description="b in TaskCopyListDictParams")

            def run(self):
                assert self.a == (1, 2)
                assert self.b == {"attr1": 1, "attr2": 2}
                assert self.a_copy == self.a
                assert self.b_copy == self.b

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        with set_luigi_config(
            {
                "TaskWithListDictParams": {
                    "a": "[1, 2]",
                    "b": json.dumps({"attr1": 1, "attr2": 2}),
                },
                "TaskCopyListDictParams": {
                    "a": "[1, 2]",
                    "b": json.dumps({"attr1": 1, "attr2": 2}),
                    "a_copy": "[1, 2]",
                    "b_copy": json.dumps({"attr1": 1, "attr2": 2}),
                },
            }
        ):
            assert luigi.build([TaskCopyListDictParams()], local_scheduler=True)

    def type_test(
        self,
        cls_param,
        initial_value,
        str_initial_value,
        new_value,
        str_new_value,
        **kwargs,
    ):
        """Test function used for different types."""

        class TaskWithTypeParams(luigi_tools.task.GlobalParamMixin, luigi.Task):
            """A task with some parameters of given type."""

            a = cls_param(description="a in TaskWithTypeParams")

            def run(self):
                assert self.a == initial_value

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        @luigi_tools.task.copy_params(
            a_copy=luigi_tools.task.ParamRef(TaskWithTypeParams, "a"),
        )
        class TaskCopyTypeParams(luigi_tools.task.GlobalParamMixin, luigi.Task):
            """A task that copies some parameters of given type."""

            a = cls_param(description="a in TaskCopyTypeParams")
            with_value = luigi.BoolParameter()

            def run(self):
                if cls_param is luigi.parameter.OptionalListParameter:
                    assert self.a == tuple(initial_value)
                else:
                    assert self.a == initial_value
                if self.with_value:
                    assert self.a_copy == new_value
                else:
                    assert self.a_copy == self.a

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        with set_luigi_config(
            {
                "TaskWithTypeParams": {
                    "a": str_initial_value,
                },
                "TaskCopyTypeParams": {
                    "a": str_initial_value,
                    "a_copy": str_new_value,
                },
            }
        ):
            assert luigi.build(
                [TaskWithTypeParams(), TaskCopyTypeParams(with_value=True)],
                local_scheduler=True,
            )

        with set_luigi_config(
            {
                "TaskWithTypeParams": {
                    "a": str_initial_value,
                },
                "TaskCopyTypeParams": {
                    "a": str_initial_value,
                },
            }
        ):
            assert luigi.build(
                [TaskWithTypeParams(), TaskCopyTypeParams(with_value=False)],
                local_scheduler=True,
            )
        print("test end with", cls_param)

    def test_int_param(self, tmp_path):
        """Test with int parameters."""
        self.type_test(luigi.parameter.IntParameter, 1, "1", 2, "2")

    def test_float_param(self, tmp_path):
        """Test with float parameters."""
        self.type_test(luigi.parameter.FloatParameter, 1.5, "1.5", 2.5, "2.5")

    def test_bool_param(self, tmp_path):
        """Test with bool parameters."""
        self.type_test(luigi_tools.parameter.BoolParameter, True, "true", False, "false")

    def test_optional_list_param(self, tmp_path):
        """Test with optional list parameters."""
        self.type_test(
            luigi.parameter.OptionalListParameter,
            (1, 2),
            "[1, 2]",
            (10, 20),
            "[10, 20]",
        )


class TestCopyParamsWithGlobals:
    """Test the `luigi_tools.task.copy_params` decorator with global parameters."""

    @pytest.mark.filterwarnings("ignore::UserWarning:luigi.parameter")
    @pytest.fixture
    def TaskA(self):
        """A simple test task."""

        class TaskA(luigi_tools.task.GlobalParamMixin, luigi.Task):
            """A simple test task."""

            a = luigi.Parameter(default="a")
            a_cfg = luigi.Parameter(default="a_cfg")

            def run(self):
                assert self.a == "a"
                assert self.a_cfg == "default_value_in_cfg"

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        return TaskA

    @pytest.mark.filterwarnings("ignore::UserWarning:luigi.parameter")
    def test_defaults(self, luigi_tools_working_directory, TaskA):
        """Test with default usage."""

        @luigi_tools.task.copy_params(
            aa=luigi_tools.task.ParamRef(TaskA, "a_cfg"),
            a_default=luigi_tools.task.ParamRef(TaskA, "a", "given_default_value"),
            a_none=luigi_tools.task.ParamRef(TaskA, "a", None),
        )
        class TaskB(luigi_tools.task.GlobalParamMixin, luigi.Task):
            """A simple test task."""

            b = luigi.Parameter(default="b")
            b_none = luigi.Parameter(default=None)
            mode = luigi.Parameter(default="default")

            def run(self):
                if self.mode == "default":
                    assert self.aa == "default_value_in_cfg"
                    assert self.a_default == "given_default_value"
                    assert self.b == "b"
                else:
                    assert self.aa == "constructor_value"
                    assert self.a_default == "new_default"
                    assert self.b == "bb"
                assert self.a_none is None
                assert self.b_none is None
                return self.aa, self.a_default, self.b

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        # Test with default value
        task = TaskB()
        res = task.run()

        assert res == ("default_value_in_cfg", "given_default_value", "b")

        # Test with another value
        task = TaskB(aa="constructor_value", a_default="new_default", b="bb", mode="constructor")
        res = task.run()

        assert res == ("constructor_value", "new_default", "bb")

        assert luigi.build([TaskA(), TaskB()], local_scheduler=True)
        assert luigi.build(
            [
                TaskA(),
                TaskB(
                    aa="constructor_value",
                    a_default="new_default",
                    b="bb",
                    mode="constructor",
                ),
            ],
            local_scheduler=True,
        )

    def test_empty(self):
        """Test with empty params.

        Empty copy_params arguments should raise a ValueError.
        """
        with pytest.raises(ValueError):

            @luigi_tools.task.copy_params()
            class TaskC(luigi.Task):
                """A simple test task."""

                a = luigi.Parameter(default="a")

    def test_duplicated(self, TaskA):
        """Test with duplicated params.

        Duplicated parameters should raise a DuplicatedParameterError.
        """
        with pytest.raises(DuplicatedParameterError):

            @luigi_tools.task.copy_params(
                a=luigi_tools.task.ParamRef(TaskA),
            )
            class TaskD(luigi_tools.task.GlobalParamMixin, luigi.Task):
                """A simple test task."""

                a = luigi.Parameter(default="a")

    @pytest.mark.filterwarnings("ignore::UserWarning:luigi.parameter")
    def test_no_default_value(self):
        """Test without default value.

        Global parameter with _no_default_value should raise GlobalParameterNoValueError.
        """

        class TaskE(luigi_tools.task.GlobalParamMixin, luigi.Task):
            """A simple test task."""

            e = luigi.Parameter(default=luigi_tools.task._no_default_value)

            def run(self):
                assert self.e == luigi_tools.task._no_default_value

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        with pytest.raises(GlobalParameterNoValueError):
            assert luigi.build([TaskE()], local_scheduler=True)

    @pytest.mark.filterwarnings("ignore::UserWarning:luigi.parameter")
    def test_copy_params_no_default_value(self, TaskA):
        """Test copied param without default value.

        Global parameter with _no_default_value should raise GlobalParameterNoValueError.
        """

        @luigi_tools.task.copy_params(
            a=luigi_tools.task.ParamRef(TaskA),
        )
        class TaskF(luigi_tools.task.GlobalParamMixin, luigi.Task):
            """A simple test task."""

            f = luigi.Parameter(default=luigi_tools.task._no_default_value)

            def run(self):
                assert self.a == "a"
                assert self.f == luigi_tools.task._no_default_value

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        with pytest.raises(GlobalParameterNoValueError):
            luigi.build([TaskF()], local_scheduler=True)

    def test_inherits(self):
        """Compare with luigi.util.inherits."""

        class TaskG(luigi.Task):
            """A simple test task."""

            g = luigi.Parameter(default="default_value_g")

            def run(self):
                assert self.g == "new_value"

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        @inherits(TaskG)
        class TaskH(luigi.Task):
            """A simple test task."""

            h = luigi.Parameter()

            def requires(self):
                return self.clone(TaskG)

            def run(self):
                assert self.g == "another_new_value"
                assert self.h == "h_from_cfg"

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        with set_luigi_config(
            {
                "TaskG": {
                    "g": "new_value",
                },
                "TaskH": {
                    "g": "another_new_value",
                    "h": "h_from_cfg",
                },
            }
        ):
            failed_task = []
            exceptions = []

            @TaskG.event_handler(luigi.Event.FAILURE)
            def check_exception(task, exception):
                failed_task.append(str(task))
                exceptions.append(str(exception))

            # The workflow fails because TaskG().g == "another_new_value" != "new_value"
            assert not luigi.build([TaskH()], local_scheduler=True)
            assert failed_task == [str(TaskG(g="another_new_value"))]
            assert exceptions == [
                "assert 'another_new_value' == 'new_value'\n  \n  - new_value\n  + "
                "another_new_value"
            ]

    def test_compare_inherits(self, luigi_tools_working_directory):
        """Compare with luigi.util.inherits."""

        class TaskI(luigi.Task):
            """A simple test task."""

            i = luigi.Parameter(default="default_value_i")

            def run(self):
                assert self.i == "new_value"
                create_not_empty_file(self.output().path)

            def output(self):
                return luigi.LocalTarget(luigi_tools_working_directory / "TaskI_output.test")

        @luigi_tools.task.copy_params(
            i=luigi_tools.task.ParamRef(TaskI),
        )
        class TaskJ(luigi.Task):
            """A simple test task."""

            j = luigi.Parameter()

            def requires(self):
                return TaskI()

            def run(self):
                assert self.i == "another_new_value"
                assert self.j == "j_from_cfg"

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        with set_luigi_config(
            {
                "TaskI": {
                    "i": "new_value",
                },
                "TaskJ": {
                    "i": "another_new_value",
                    "j": "j_from_cfg",
                },
            }
        ):
            # The workflow now succeeds because TaskI().i == "new_value" and
            # TaskJ().i == "another_new_value"
            assert luigi.build([TaskJ()], local_scheduler=True)

    def test_serialized_parameters(self):
        """Test with parameters that are serialized to generate the task ID."""

        class GlobalParamTaskWithListDictParams(luigi_tools.task.GlobalParamMixin, luigi.Task):
            """A simple test task."""

            a = luigi.ListParameter(default="a in GlobalParamTaskWithListDictParams")
            b = luigi.DictParameter(default="b in GlobalParamTaskWithListDictParams")

            def run(self):
                assert self.a == (1, 2)
                assert self.b == {"attr1": 1, "attr2": 2}

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        @luigi_tools.task.copy_params(
            a_copy=luigi_tools.task.ParamRef(GlobalParamTaskWithListDictParams, "a"),
            b_copy=luigi_tools.task.ParamRef(GlobalParamTaskWithListDictParams, "b"),
        )
        class GlobalParamTaskCopyListDictParams(luigi_tools.task.GlobalParamMixin, luigi.Task):
            """A simple test task."""

            a_new = luigi.ListParameter(default="a in GlobalParamTaskCopyListDictParams")
            b_new = luigi.DictParameter(default="b in GlobalParamTaskCopyListDictParams")

            def run(self):
                assert self.a_new == (1, 2)
                assert self.b_new == {"attr1": 1, "attr2": 2}
                assert self.a_copy == self.a_new
                assert self.b_copy == self.b_new

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        with set_luigi_config(
            {
                "GlobalParamTaskWithListDictParams": {
                    "a": "[1, 2]",
                    "b": json.dumps({"attr1": 1, "attr2": 2}),
                },
                "GlobalParamTaskCopyListDictParams": {
                    "a_new": "[1, 2]",
                    "b_new": json.dumps({"attr1": 1, "attr2": 2}),
                },
            }
        ):
            assert luigi.build(
                [
                    GlobalParamTaskWithListDictParams(),
                    GlobalParamTaskCopyListDictParams(),
                ],
                local_scheduler=True,
            )

    def test_not_comparable_attribute(self):
        """Test with params that can't be compared."""

        class NotComparable:
            """A class that always raises an exception when __eq__ is called."""

            def __eq__(self, other):
                raise TypeError(f"Can't compare {self.__class__} with {type(other)}")

        class GlobalParamTaskSetGetAttr(luigi_tools.task.GlobalParamMixin, luigi.Task):
            """A simple test task."""

            a = luigi.ListParameter(default="a in GlobalParamTaskSetGetAttr")
            b = None
            c = 1

            def run(self):
                assert self.a == "a in GlobalParamTaskSetGetAttr"
                self.a = NotComparable()
                assert isinstance(self.a, NotComparable)
                self.a = "a in GlobalParamTaskSetGetAttr"
                assert self.a == "a in GlobalParamTaskSetGetAttr"

                assert self.b is None
                self.b = NotComparable()
                assert isinstance(self.b, NotComparable)
                self.b = None
                assert self.b is None

                assert self.c == 1
                self.c = NotComparable()
                assert isinstance(self.c, NotComparable)
                self.c = 1
                assert self.c == 1

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        assert luigi.build(
            [GlobalParamTaskSetGetAttr()],
            local_scheduler=True,
        )

    def test_task_value(self):
        """Test monkey patch of task_value().

        This test fails without monkey patch because a list parameter can not be None.
        """

        class TaskListParameter(luigi_tools.task.GlobalParamMixin, luigi.Task):
            """A simple test task."""

            a = luigi.parameter.ListParameter(default=(1, 2))
            b = luigi.parameter.ListParameter()

            def run(self):
                assert self.a == (1, 2)
                assert self.b == tuple()

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        @luigi_tools.task.copy_params(
            a=luigi_tools.task.ParamRef(TaskListParameter),
            b=luigi_tools.task.ParamRef(TaskListParameter),
        )
        class TaskCopyListParameter(luigi_tools.task.GlobalParamMixin, luigi.Task):
            """A simple test task."""

            def run(self):
                assert self.a == (1, 2)
                assert self.b == tuple()

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        with set_luigi_config({"TaskListParameter": {"b": []}, "TaskCopyListParameter": {"b": []}}):
            assert luigi.build([TaskListParameter(), TaskCopyListParameter()], local_scheduler=True)

    @pytest.mark.skipif(
        Version(importlib.metadata.version("luigi")) < Version("3.2"),
        reason="Schemas are only available for luigi>=3.2",
    )
    def test_dict_param_with_schema(self):
        """Test interaction of @copy_params and schemas."""

        class TaskWithListDictParams(luigi.Config):
            """A simple config task."""

            a = luigi.OptionalListParameter(
                description="a in TaskWithListDictParams", schema={"type": "array"}
            )
            b = luigi.OptionalDictParameter(
                description="a in TaskWithListDictParams", schema={"type": "object"}
            )

        @luigi_tools.task.copy_params(
            a=luigi_tools.task.ParamRef(TaskWithListDictParams),
            b=luigi_tools.task.ParamRef(TaskWithListDictParams),
        )
        class TaskCopyListDictParams(luigi_tools.task.GlobalParamMixin, luigi.Task):
            """A simple test task."""

            def run(self):
                assert self.a == (1, 2)
                assert self.b == {"attr1": 1, "attr2": 2}

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        with set_luigi_config(
            {
                "TaskWithListDictParams": {
                    "a": "[1, 2]",
                    "b": json.dumps({"attr1": 1, "attr2": 2}),
                }
            }
        ):
            assert luigi.build([TaskCopyListDictParams()], local_scheduler=True)


class TestForceableTask:
    """Test the rerun feature."""

    def test_no_rerun(self, task_collection):
        """Test that everything is run when all rerun are False and targets are missing."""
        assert all(
            not check_existing_file(i.path) for i in luigi.task.flatten(task_collection.targets)
        )

        assert luigi.build([task_collection.TaskE()], local_scheduler=True)

        assert all(
            check_not_empty_file(i.path) for i in luigi.task.flatten(task_collection.targets)
        )

    def test_no_rerun_with_complete_targets(self, task_collection):
        """Test that nothing is run when all rerun are False and targets are present."""
        for i in luigi.task.flatten(task_collection.targets):
            create_empty_file(i.path)

        assert luigi.build([task_collection.TaskE()], local_scheduler=True)

        assert all(check_empty_file(i.path) for i in luigi.task.flatten(task_collection.targets))

    def test_rerun_with_complete_targets(self, task_collection):
        """Test that everything is run when rerun = True for the root task.

        The targets of all tasks are present and should thus be automatically removed before running
        the workflow.
        """
        for i in luigi.task.flatten(task_collection.targets):
            create_empty_file(i.path)

        with set_luigi_config(
            {
                "TaskA": {"rerun": "true"},
            }
        ):
            assert luigi.build([task_collection.TaskE()], local_scheduler=True)

        assert all(
            check_not_empty_file(i.path) for i in luigi.task.flatten(task_collection.targets)
        )

    def test_rerun_parents_only(self, task_collection):
        """Test that only the parents of the task with rerun = True are run."""
        for i in luigi.task.flatten(task_collection.targets):
            create_empty_file(i.path)

        with set_luigi_config(
            {
                "TaskB": {"rerun": "true"},
            }
        ):
            assert luigi.build([task_collection.TaskE()], local_scheduler=True)

        assert all(
            check_not_empty_file(j.path)
            for task_name, targets in task_collection.targets.items()
            for j in luigi.task.flatten(targets)
            if task_name not in ["TaskA", "TaskC"]
        )
        assert all(
            check_empty_file(j.path)
            for task_name, targets in task_collection.targets.items()
            for j in luigi.task.flatten(targets)
            if task_name in ["TaskA", "TaskC"]
        )

    def test_no_remove_if_recall(self, task_collection, tmpdir):
        """Test that calling a task inside another one does not remove its targets."""

        class TaskF(luigi_tools.task.WorkflowTask):
            """A simple test task."""

            counter = luigi.IntParameter(default=0)
            rerun = luigi.BoolParameter()

            def requires(self):
                return task_collection.TaskE()

            def run(self):
                # Call A inside F but the targets of A should not be removed
                _ = task_collection.TaskA(counter=999)

                for i in luigi.task.flatten(self.output()):
                    create_not_empty_file(i.path)

            def output(self):
                return {
                    "first_target": luigi.LocalTarget(tmpdir / "TaskF.target"),
                    "other_targets": {
                        "second_target": luigi.LocalTarget(tmpdir / "TaskE2.target"),
                        "third_target": luigi.LocalTarget(tmpdir / "TaskE3.target"),
                    },
                }

        for i in luigi.task.flatten(task_collection.targets):
            create_empty_file(i.path)

        with set_luigi_config(
            {
                "TaskB": {"rerun": "true"},
            }
        ):
            assert luigi.build([TaskF()], local_scheduler=True)

        assert all(
            check_not_empty_file(j.path)
            for task_name, targets in task_collection.targets.items()
            for j in luigi.task.flatten(targets)
            if task_name not in ["TaskA", "TaskC"]
        )
        assert all(
            check_empty_file(j.path)
            for task_name, targets in task_collection.targets.items()
            for j in luigi.task.flatten(targets)
            if task_name in ["TaskA", "TaskC"]
        )

    def test_wrapper_task(self, task_collection, tmpdir):
        """Test that it is possible to rerun a wrapper task and all its dependencies."""

        class TaskF(luigi_tools.task.WorkflowWrapperTask):
            """A simple test wrapper task."""

            def requires(self):
                return task_collection.TaskE()

        for i in luigi.task.flatten(task_collection.targets):
            create_empty_file(i.path)

        with set_luigi_config(
            {
                "TaskF": {"rerun": "true"},
            }
        ):
            assert luigi.build([TaskF()], local_scheduler=True)

        assert all(
            check_not_empty_file(j.path)
            for task_name, targets in task_collection.targets.items()
            for j in luigi.task.flatten(targets)
        )


class TestLogTargetMixin:
    """Test the `luigi_tools.task.LogTargetMixin` class."""

    def test_output_logger(self, tmpdir, caplog):
        """Test the logger outputs with one target."""

        class TaskA(luigi_tools.task.LogTargetMixin, luigi.Task):
            """A simple test task."""

            a = luigi.Parameter(default="a")

            def run(self):
                create_empty_file(self.output().path)

            def output(self):
                return luigi.LocalTarget(tmpdir / f"test_{self.a}")

        caplog.clear()
        caplog.set_level(logging.DEBUG)
        assert luigi.build([TaskA()], local_scheduler=True)
        res = [i for i in caplog.record_tuples if i[0] == "luigi_tools.task"]
        assert res == [("luigi_tools.task", 10, f"Output of TaskA task: {tmpdir / 'test_a'}")]

    def test_output_logger_with_dict(self, tmpdir, caplog):
        """Test the logger outputs with a dict target."""

        class AlwaysExistingTarget(luigi.Target):
            """A target that is always considered as existing."""

            def exists(self):
                return True

        class TaskB(luigi_tools.task.LogTargetMixin, luigi.Task):
            """A simple test task."""

            b = luigi.Parameter(default="b")

            def run(self):
                create_empty_file(self.output()["b"].path)

            def output(self):
                return {
                    "b": luigi.LocalTarget(tmpdir / f"test_{self.b}"),
                    "dummy": AlwaysExistingTarget(),
                }

        caplog.clear()
        caplog.set_level(logging.DEBUG)
        assert luigi.build([TaskB()], local_scheduler=True)
        res = [i for i in caplog.record_tuples if i[0] == "luigi_tools.task"]
        assert res == [("luigi_tools.task", 10, f"Output b of TaskB task: {tmpdir / 'test_b'}")]

        class TaskC(luigi.Task):
            """A simple test task."""

            c = luigi.Parameter(default="c")

            def run(self):
                create_empty_file(self.output().path)

            def output(self):
                return luigi.LocalTarget(tmpdir / f"test_{self.c}")

        # Test that other classes do not emit this log entry
        caplog.clear()
        caplog.set_level(logging.DEBUG)
        assert luigi.build([TaskC()], local_scheduler=True)
        res = [i for i in caplog.record_tuples if i[0] == "luigi_tools.task"]
        assert res == []

    def test_output_logger_ignored(self, tmpdir, caplog):
        """Test that there is no logger output when the mixin is located after `luigi.Task`."""

        class TaskD(luigi.Task, luigi_tools.task.LogTargetMixin):
            """A simple test task."""

            d = luigi.Parameter(default="d")

            def run(self):
                create_empty_file(self.output().path)

            def output(self):
                return luigi.LocalTarget(tmpdir / f"test_{self.d}")

        # Test that no log entry is emitted when the mixin is placed after any luigi.Task
        caplog.clear()
        caplog.set_level(logging.DEBUG)
        assert luigi.build([TaskD()], local_scheduler=True)
        res = [i for i in caplog.record_tuples if i[0] == "luigi_tools.task"]
        assert res == []

    def test_output_logger_and_global_param(self, tmpdir, caplog):
        """Test logger outputs when interacting with `luigi_tools.task.GlobalParamMixin`."""

        class TaskE(
            luigi_tools.task.LogTargetMixin,
            luigi_tools.task.GlobalParamMixin,
            luigi.Task,
        ):
            """A simple test task."""

            e = luigi.Parameter(default="e")

            def run(self):
                create_empty_file(self.output().path)

            def output(self):
                return luigi.LocalTarget(tmpdir / f"test_{self.e}")

        # Test that combining several mixins works as intended (i.e. all log entries appear)
        caplog.clear()
        caplog.set_level(logging.DEBUG)
        assert luigi.build([TaskE()], local_scheduler=True)
        res = [i for i in caplog.record_tuples if i[0] == "luigi_tools.task"]
        assert res == [
            (
                "luigi_tools.task",
                10,
                "Attributes of TaskE task after global processing:",
            ),
            ("luigi_tools.task", 10, "Attribute: e == e"),
            ("luigi_tools.task", 10, f"Output of TaskE task: {tmpdir / 'test_e'}"),
        ]


def test_remove_corrupted_output(tmpdir, caplog):
    """Test the `luigi_tools.task.RemoveCorruptedOutputMixin` mixin."""

    class TaskToFail(luigi_tools.task.RemoveCorruptedOutputMixin, luigi.Task):
        """A Task that's expected to fail and create incomplete/wrong output."""

        def output(self):
            return luigi.LocalTarget(tmpdir / "matrix.dat")

        def run(self):
            f_path = self.output().path
            with open(f_path, "w", encoding="utf-8") as f_handle:
                for idx in range(10):
                    f_handle.write(f"{idx}\n")
                    if idx >= 5:
                        raise RuntimeError("something unexpected happened!")

    @TaskToFail.event_handler(luigi.Event.FAILURE)
    def check_exception(task, exception):
        """Gets triggered upon luigi.Event.FAILURE event."""
        assert isinstance(exception, RuntimeError)
        assert exception.args[0] == "something unexpected happened!"

    task_instance = TaskToFail(clean_failed=True)
    caplog.clear()
    caplog.set_level(logging.DEBUG)
    luigi.build([task_instance], local_scheduler=True)

    # there are missing targets
    assert not task_instance.complete()

    res = [i for i in caplog.record_tuples if i[0] == "luigi_tools.util"]
    assert res == [
        ("luigi_tools.util", 10, f"Removing {tmpdir / 'matrix.dat'}"),
    ]

    assert not (tmpdir / "matrix.dat").exists()

    task_instance.clean_failed = False
    luigi.build([task_instance], local_scheduler=True)

    # all targets are produced
    assert task_instance.complete()

    with open(tmpdir / "matrix.dat", "r", encoding="utf-8") as f_handle:
        matrix_values = f_handle.read().splitlines()
        assert len(matrix_values) == 6


class TestCheckUnconsumedParams:
    """Check that warnings are properly emitted when a parameter is not consumed by a task."""

    def test_unconsumed(self, tmpdir, caplog):
        """Simple test with unconsumed parameters."""

        class TaskA(luigi_tools.task.WorkflowTask):
            """A simple test task."""

            a = luigi.Parameter(default="a")

            def run(self):
                create_not_empty_file(self.output().path)

            def output(self):
                return luigi.LocalTarget(tmpdir / f"test_TaskA_{self.a}")

        class TaskB(luigi_tools.task.WorkflowTask):
            """A simple test task."""

            a = luigi.Parameter(default="a")

            def run(self):
                create_not_empty_file(self.output().path)

            def output(self):
                return luigi.LocalTarget(tmpdir / f"test_TaskB_{self.a}")

        with set_luigi_config(
            {
                "TaskA": {
                    "a": "a",
                    "b": "b",
                    "c": "c",
                },
                "TaskB": {
                    "a": "a",
                    "b": "b",
                    "c": "c",
                },
            }
        ):
            with warnings.catch_warnings(record=True) as w:
                warnings.filterwarnings(
                    action="ignore",
                    category=Warning,
                )
                warnings.simplefilter(
                    action="always",
                    category=luigi.parameter.UnconsumedParameterWarning,
                )
                assert luigi.build([TaskA(), TaskB()], local_scheduler=True)

                assert len(w) == 4
                expected = [
                    ("b", "TaskA"),
                    ("c", "TaskA"),
                    ("b", "TaskB"),
                    ("c", "TaskB"),
                ]
                for i, (expected_value, task_name) in zip(w, expected):
                    assert issubclass(i.category, luigi.parameter.UnconsumedParameterWarning)
                    assert str(i.message) == (
                        "The configuration contains the parameter "
                        f"'{expected_value}' with value '{expected_value}' that is not consumed by "
                        f"the task '{task_name}'."
                    )

        assert all(check_not_empty_file(i.path) for i in luigi.task.flatten(TaskA().output()))
