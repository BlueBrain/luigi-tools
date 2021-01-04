import os

import luigi
import pytest

import luigi_tools.task

from .tools import create_not_empty_file
from .tools import dict_to_config
from .tools import set_luigi_config


@pytest.fixture(scope="function")
def tmp_working_dir(tmp_path):
    """Change working directory before a test and change it back when the test is finished"""
    cwd = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(cwd)


@pytest.fixture
def luigi_tools_params():
    return dict_to_config({"TaskA": {"a_cfg": "default_value_in_cfg"}})


@pytest.fixture
def luigi_tools_working_directory(tmp_working_dir, luigi_tools_params):
    # Setup config
    params = luigi_tools_params

    # Set config
    with set_luigi_config(params):
        yield tmp_working_dir


@pytest.fixture
def TasksFixture(tmpdir):
    class TaskClasses:
        """Class with some luigi tasks to test"""

        def __init__(self):
            self.tmpdir = tmpdir
            self.reset_classes()

        def reset_classes(self):
            class TaskA(luigi_tools.task.WorkflowTask):
                """"""

                counter = luigi.IntParameter(default=0)
                rerun = luigi.BoolParameter()

                def run(self):
                    for i in luigi.task.flatten(self.output()):
                        create_not_empty_file(i.path)

                def output(self):
                    return luigi.LocalTarget(tmpdir / "TaskA.target")

            class TaskB(luigi_tools.task.WorkflowTask):
                """"""

                counter = luigi.IntParameter(default=0)
                rerun = luigi.BoolParameter()

                def requires(self):
                    return TaskA()

                def run(self):
                    for i in luigi.task.flatten(self.output()):
                        create_not_empty_file(i.path)

                def output(self):
                    return [
                        luigi.LocalTarget(tmpdir / "TaskB.target"),
                        [
                            luigi.LocalTarget(tmpdir / "TaskB2.target"),
                            luigi.LocalTarget(tmpdir / "TaskB3.target"),
                        ],
                    ]

            class TaskC(luigi_tools.task.WorkflowTask):
                """"""

                counter = luigi.IntParameter(default=0)
                rerun = luigi.BoolParameter()

                def requires(self):
                    return TaskA()

                def run(self):
                    for i in luigi.task.flatten(self.output()):
                        create_not_empty_file(i.path)

                def output(self):
                    return {
                        "first_target": luigi.LocalTarget(tmpdir / "TaskC.target"),
                        "second_target": luigi.LocalTarget(tmpdir / "TaskC2.target"),
                    }

            class TaskD(luigi_tools.task.WorkflowTask):
                """"""

                counter = luigi.IntParameter(default=0)
                rerun = luigi.BoolParameter()

                def requires(self):
                    return [TaskB(), TaskC()]

                def run(self):
                    for i in luigi.task.flatten(self.output()):
                        create_not_empty_file(i.path)

                def output(self):
                    return [
                        luigi.LocalTarget(tmpdir / "TaskD.target"),
                        luigi.LocalTarget(tmpdir / "TaskD2.target"),
                    ]

            class TaskE(luigi_tools.task.WorkflowTask):
                """"""

                counter = luigi.IntParameter(default=0)
                rerun = luigi.BoolParameter()

                def requires(self):
                    return TaskD()

                def run(self):
                    for i in luigi.task.flatten(self.output()):
                        create_not_empty_file(i.path)

                def output(self):
                    return {
                        "first_target": luigi.LocalTarget(tmpdir / "TaskE.target"),
                        "other_targets": {
                            "second_target": luigi.LocalTarget(tmpdir / "TaskE2.target"),
                            "third_target": luigi.LocalTarget(tmpdir / "TaskE3.target"),
                        },
                    }

            self.TaskA = TaskA
            self.TaskB = TaskB
            self.TaskC = TaskC
            self.TaskD = TaskD
            self.TaskE = TaskE
            self.counter = 0

        @property
        def classes(self):
            return [
                self.TaskA,
                self.TaskB,
                self.TaskC,
                self.TaskD,
                self.TaskE,
            ]

    return TaskClasses
