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

"""Fixtures for luigi-tools test suite."""
import os

import luigi
import pytest

import luigi_tools.task
from luigi_tools.util import set_luigi_config

from .tools import create_not_empty_file


@pytest.fixture(scope="function")
def tmp_working_dir(tmp_path):
    """Change working directory before a test and change it back when the test is finished"""
    cwd = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(cwd)


@pytest.fixture
def luigi_tools_params():
    return {"TaskA": {"a_cfg": "default_value_in_cfg"}}


@pytest.fixture
def luigi_tools_working_directory(tmp_working_dir, luigi_tools_params):
    # Set config
    with set_luigi_config(luigi_tools_params):
        yield tmp_working_dir


@pytest.fixture
def task_collection(tmpdir):
    class TaskClasses:
        """Class with some luigi tasks to test"""

        def __init__(self):
            self.tmpdir = tmpdir
            self.reset_classes()
            self.classes = self._classes()
            self.targets = self._targets()
            self.reset_classes()  # Reset again to return classes that are not registered by luigi

        def reset_classes(self):
            class TaskA(luigi_tools.task.WorkflowTask):
                """"""

                counter = luigi.IntParameter(default=0)

                def run(self):
                    for i in luigi.task.flatten(self.output()):
                        create_not_empty_file(i.path)

                def output(self):
                    return luigi.LocalTarget(tmpdir / "TaskA.target")

            class TaskB(luigi_tools.task.WorkflowTask):
                """"""

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

        def _classes(self):
            return [
                self.TaskA,
                self.TaskB,
                self.TaskC,
                self.TaskD,
                self.TaskE,
            ]

        def _targets(self):
            targets = {}
            for task in self.classes:
                targets[task.__name__] = task().output()
            return targets

    return TaskClasses()
