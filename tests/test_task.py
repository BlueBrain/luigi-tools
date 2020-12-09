"""Tests for luigi tools."""
import json
import logging

import luigi
import pytest
from luigi.util import inherits

import luigi_tools.task
import luigi_tools.target
import luigi_tools.util
from luigi_tools.task import DuplicatedParameterError
from luigi_tools.task import GlobalParameterNoValueError

from .tools import check_empty_file
from .tools import check_not_empty_file
from .tools import create_empty_file
from .tools import create_not_empty_file
from .tools import set_luigi_config


@pytest.mark.filterwarnings("ignore::UserWarning:luigi.parameter")
def test_copy_params(tmpdir):
    class TaskA(luigi.Task):
        """"""

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
        """"""

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
            """"""

            a = luigi.Parameter(default="a")

    # Duplicated parameters should raise a DuplicatedParameterError
    with pytest.raises(DuplicatedParameterError):

        @luigi_tools.task.copy_params(
            a=luigi_tools.task.ParamRef(TaskA),
        )
        class TaskD(luigi.Task):
            """"""

            a = luigi.Parameter(default="a")

    # Test with parameters that are serialized to generate the task ID
    class TaskWithListDictParams(luigi.Task):
        """"""

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
        """"""

        a = luigi.ListParameter(description="a in TaskCopyListDictParams")
        b = luigi.DictParameter(description="b in TaskCopyListDictParams")

        def run(self):
            assert self.a == (1, 2)
            assert self.b == {"attr1": 1, "attr2": 2}
            assert self.a_copy == self.a
            assert self.b_copy == self.b

        def output(self):
            return luigi.LocalTarget("not_existing_file")

    set_luigi_config(
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
    )
    assert luigi.build([TaskCopyListDictParams()], local_scheduler=True)


@pytest.mark.filterwarnings("ignore::UserWarning:luigi.parameter")
def test_copy_params_with_globals(luigi_tools_working_directory):
    class TaskA(luigi_tools.task.GlobalParamMixin, luigi.Task):
        """"""

        a = luigi.Parameter(default="a")
        a_cfg = luigi.Parameter(default="a_cfg")

        def run(self):
            assert self.a == "a"
            assert self.a_cfg == "default_value_in_cfg"

        def output(self):
            return luigi.LocalTarget("not_existing_file")

    @luigi_tools.task.copy_params(
        aa=luigi_tools.task.ParamRef(TaskA, "a_cfg"),
        a_default=luigi_tools.task.ParamRef(TaskA, "a", "given_default_value"),
        a_none=luigi_tools.task.ParamRef(TaskA, "a", None),
    )
    class TaskB(luigi_tools.task.GlobalParamMixin, luigi.Task):
        """"""

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

    # Empty copy_params arguments should raise a ValueError
    with pytest.raises(ValueError):

        @luigi_tools.task.copy_params()
        class TaskC(luigi.Task):
            """"""

            a = luigi.Parameter(default="a")

    # Duplicated parameters should raise a DuplicatedParameterError
    with pytest.raises(DuplicatedParameterError):

        @luigi_tools.task.copy_params(
            a=luigi_tools.task.ParamRef(TaskA),
        )
        class TaskD(luigi_tools.task.GlobalParamMixin, luigi.Task):
            """"""

            a = luigi.Parameter(default="a")

    # Global parameter with _no_default_value should raise GlobalParameterNoValueError
    class TaskE(luigi_tools.task.GlobalParamMixin, luigi.Task):
        """"""

        e = luigi.Parameter(default=luigi_tools.task._no_default_value)

        def run(self):
            assert self.e == luigi_tools.task._no_default_value

        def output(self):
            return luigi.LocalTarget("not_existing_file")

    with pytest.raises(GlobalParameterNoValueError):
        assert luigi.build([TaskE()], local_scheduler=True)

    # Global parameter with _no_default_value should raise GlobalParameterNoValueError
    @luigi_tools.task.copy_params(
        a=luigi_tools.task.ParamRef(TaskA),
    )
    class TaskF(luigi_tools.task.GlobalParamMixin, luigi.Task):
        """"""

        f = luigi.Parameter(default=luigi_tools.task._no_default_value)

        def run(self):
            assert self.a == "a"
            assert self.f == luigi_tools.task._no_default_value

        def output(self):
            return luigi.LocalTarget("not_existing_file")

    with pytest.raises(GlobalParameterNoValueError):
        assert luigi.build([TaskF()], local_scheduler=True)

    # Compare with luigi.util.inherits
    class TaskG(luigi.Task):
        """"""

        g = luigi.Parameter(default="default_value_g")

        def run(self):
            assert self.g == "new_value"

        def output(self):
            return luigi.LocalTarget("not_existing_file")

    @inherits(TaskG)
    class TaskH(luigi.Task):
        """"""

        h = luigi.Parameter()

        def requires(self):
            return self.clone(TaskG)

        def run(self):
            assert self.g == "another_new_value"
            assert self.h == "h_from_cfg"

        def output(self):
            return luigi.LocalTarget("not_existing_file")

    set_luigi_config(
        {
            "TaskG": {
                "g": "new_value",
            },
            "TaskH": {
                "g": "another_new_value",
                "h": "h_from_cfg",
            },
        }
    )
    with pytest.raises(AssertionError):
        # The workflow fails because TaskG().g == "another_new_value" != "new_value"
        assert luigi.build([TaskH()], local_scheduler=True)

    # Compare with luigi.util.inherits
    class TaskI(luigi.Task):
        """"""

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
        """"""

        j = luigi.Parameter()

        def requires(self):
            return TaskI()

        def run(self):
            assert self.i == "another_new_value"
            assert self.j == "j_from_cfg"

        def output(self):
            return luigi.LocalTarget("not_existing_file")

    set_luigi_config(
        {
            "TaskI": {
                "i": "new_value",
            },
            "TaskJ": {
                "i": "another_new_value",
                "j": "j_from_cfg",
            },
        }
    )
    # The workflow now succeeds because TaskI().i == "new_value" and
    # TaskJ().i == "another_new_value"
    assert luigi.build([TaskJ()], local_scheduler=True)

    # Test with parameters that are serialized to generate the task ID
    class GlobalParamTaskWithListDictParams(luigi_tools.task.GlobalParamMixin, luigi.Task):
        """"""

        a = luigi.ListParameter(description="a in GlobalParamTaskWithListDictParams")
        b = luigi.DictParameter(description="b in GlobalParamTaskWithListDictParams")

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
        """"""

        a_new = luigi.ListParameter(description="a in GlobalParamTaskCopyListDictParams")
        b_new = luigi.DictParameter(description="b in GlobalParamTaskCopyListDictParams")

        def run(self):
            assert self.a_new == (1, 2)
            assert self.b_new == {"attr1": 1, "attr2": 2}
            assert self.a_copy == self.a_new
            assert self.b_copy == self.b_new

        def output(self):
            return luigi.LocalTarget("not_existing_file")

    set_luigi_config(
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
    )
    assert luigi.build(
        [GlobalParamTaskWithListDictParams(), GlobalParamTaskCopyListDictParams()],
        local_scheduler=True,
    )


def test_forceable_tasks(tmpdir, TasksFixture):
    all_tasks = TasksFixture()

    all_targets = {}
    for task in all_tasks.classes:
        all_targets[task.__name__] = task().output()

    # Test that everything is run when all rerun are False and targets are missing
    print("=================== FIRST BUILD ====================")
    all_tasks.reset_classes()

    assert luigi.build([all_tasks.TaskE()], local_scheduler=True)

    assert all([check_not_empty_file(i.path) for i in luigi.task.flatten(all_targets)])

    # Test that nothing is run when all rerun are False and targets are present
    for i in luigi.task.flatten(all_targets):
        create_empty_file(i.path)

    print("=================== SECOND BUILD ====================")
    all_tasks.reset_classes()
    assert luigi.build([all_tasks.TaskE()], local_scheduler=True)

    assert all([check_empty_file(i.path) for i in luigi.task.flatten(all_targets)])

    # Test that everything is run when rerun = True for the root task and targets are present
    for i in luigi.task.flatten(all_targets):
        create_empty_file(i.path)

    print("=================== THIRD BUILD ====================")
    all_tasks.reset_classes()
    all_tasks.TaskA.rerun = luigi.BoolParameter(default=True)
    assert luigi.build([all_tasks.TaskE()], local_scheduler=True)

    assert all([check_not_empty_file(i.path) for i in luigi.task.flatten(all_targets)])

    # Test that only the parents of the task with rerun = True are run
    for i in luigi.task.flatten(all_targets):
        create_empty_file(i.path)

    print("=================== FORTH BUILD ====================")
    all_tasks.reset_classes()
    all_tasks.TaskB.rerun = luigi.BoolParameter(default=True)
    assert luigi.build([all_tasks.TaskE()], local_scheduler=True)

    assert all(
        [
            check_not_empty_file(i.path)
            for task_name, targets in all_targets.items()
            for j in luigi.task.flatten(targets)
            if task_name not in ["TaskA", "TaskC"]
        ]
    )
    assert all(
        [
            check_empty_file(j.path)
            for task_name, targets in all_targets.items()
            for j in luigi.task.flatten(targets)
            if task_name in ["TaskA", "TaskC"]
        ]
    )

    # Test that calling a task inside another one does not remove its targets
    all_tasks.reset_classes()

    class TaskF(luigi_tools.task.WorkflowTask):
        """"""

        counter = luigi.IntParameter(default=0)
        rerun = luigi.BoolParameter()

        def requires(self):
            return all_tasks.TaskE()

        def run(self):
            # Call A inside F but the targets of A should not be removed
            _ = all_tasks.TaskA(counter=999)

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

    for i in luigi.task.flatten(all_targets):
        create_empty_file(i.path)

    print("=================== FIFTH BUILD ====================")
    all_tasks.TaskB.rerun = luigi.BoolParameter(default=True)
    assert luigi.build([TaskF()], local_scheduler=True)

    assert all(
        [
            check_not_empty_file(i.path)
            for task_name, targets in all_targets.items()
            for j in luigi.task.flatten(targets)
            if task_name not in ["TaskA", "TaskC"]
        ]
    )
    assert all(
        [
            check_empty_file(j.path)
            for task_name, targets in all_targets.items()
            for j in luigi.task.flatten(targets)
            if task_name in ["TaskA", "TaskC"]
        ]
    )


def test_LogTargetMixin(tmpdir, caplog):
    class TaskA(luigi_tools.task.LogTargetMixin, luigi.Task):
        """"""

        a = luigi.Parameter(default="a")

        def run(self):
            create_empty_file(self.output().path)

        def output(self):
            return luigi.LocalTarget(tmpdir / f"test_{self.a}")

    class AlwaysExistingTarget(luigi.Target):
        def exists(self):
            return True

    class TaskB(luigi_tools.task.LogTargetMixin, luigi.Task):
        """"""

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
    assert luigi.build([TaskA()], local_scheduler=True)
    res = [i for i in caplog.record_tuples if i[0] == "luigi_tools.task"]
    assert res == [("luigi_tools.task", 10, f"Output of TaskA task: {tmpdir / 'test_a'}")]

    caplog.clear()
    caplog.set_level(logging.DEBUG)
    assert luigi.build([TaskB()], local_scheduler=True)
    res = [i for i in caplog.record_tuples if i[0] == "luigi_tools.task"]
    assert res == [("luigi_tools.task", 10, f"Output b of TaskB task: {tmpdir / 'test_b'}")]

    class TaskC(luigi.Task):
        """"""

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

    class TaskD(luigi.Task, luigi_tools.task.LogTargetMixin):
        """"""

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

    class TaskE(luigi_tools.task.LogTargetMixin, luigi_tools.task.GlobalParamMixin, luigi.Task):
        """"""

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
        ("luigi_tools.task", 10, "Attributes of TaskE task after global processing:"),
        ("luigi_tools.task", 10, "Atribute: e == e"),
        ("luigi_tools.task", 10, f"Output of TaskE task: {tmpdir / 'test_e'}"),
    ]