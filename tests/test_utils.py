"""Tests for luigi tools."""
import os

import luigi
from luigi.parameter import _no_value as PARAM_NO_VALUE
import pytest

import luigi_tools.tasks
import luigi_tools.targets
import luigi_tools.utils

from .tools import check_empty_file
from .tools import check_not_empty_file
from .tools import create_empty_file
from .tools import create_not_empty_file


def test_target_remove(tmpdir):
    class TaskA(luigi_tools.tasks.WorkflowTask):
        """"""

        def run(self):
            for i in luigi.task.flatten(self.output()):
                i.makedirs()

            os.makedirs(self.output()[0].path)
            create_not_empty_file(self.output()[0].path + "/file.test")
            create_not_empty_file(self.output()[1].path)

            for i in luigi.task.flatten(self.output()):
                assert i.exists()
                luigi_tools.utils.target_remove(i)
                assert not i.exists()

        def output(self):
            return [
                luigi.LocalTarget(tmpdir / "TaskA"),
                luigi.LocalTarget(tmpdir / "TaskA_bis" / "file.test"),
            ]

    assert luigi.build([TaskA()], local_scheduler=True)


def test_apply_over_inputs():
    class TaskA(luigi.Task):
        a = luigi.Parameter(default="a")

        def output(self):
            return self.a

    class TaskB(luigi.Task):
        def requires(self):
            return {
                "a": TaskA(),
                "b": TaskA(a="b"),
                "c": [TaskA(a="c1"), TaskA(a="c2")],
            }

    def get_a(task_output, key=None):
        return key, task_output

    task = TaskB()
    res = luigi_tools.utils.apply_over_inputs(task, get_a)
    assert res == {"a": ("a", "a"), "b": ("b", "b"), "c": ("c", ["c1", "c2"])}


def test_apply_over_outputs():
    class TaskA(luigi.Task):
        a = luigi.Parameter(default="a")

        def output(self):
            return {
                "a": self.a,
                "b": self.a + "_b",
                "c": [self.a + "_c1", self.a + "_c2"],
            }

    def get_a(task_output, key=None):
        return key, task_output

    task = TaskA(a="test")
    res = luigi_tools.utils.apply_over_outputs(task, get_a)
    assert res == {
        "a": ("a", "test"),
        "b": ("b", "test_b"),
        "c": ("c", ["test_c1", "test_c2"]),
    }


def test_dependency_graph(tmpdir, TasksFixture):
    all_tasks = TasksFixture()
    start = all_tasks.TaskE()

    graph = luigi_tools.utils.get_dependency_graph(start)
    assert graph == [
        (all_tasks.TaskE(), all_tasks.TaskD()),
        (all_tasks.TaskD(), all_tasks.TaskB()),
        (all_tasks.TaskB(), all_tasks.TaskA()),
        (all_tasks.TaskD(), all_tasks.TaskC()),
        (all_tasks.TaskC(), all_tasks.TaskA()),
    ]


def test_param_repr():
    assert luigi_tools.utils._param_repr(None, PARAM_NO_VALUE) == ""
    assert luigi_tools.utils._param_repr(None, None) == "(None)"
    assert luigi_tools.utils._param_repr("description", None) == "description(None)"
    assert luigi_tools.utils._param_repr("description", "default") == "description(default)"
