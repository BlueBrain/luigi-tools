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

"""Configuration for the pytest test suite."""

# pylint: disable=empty-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
import luigi
import pytest

import luigi_tools.task
from luigi_tools.util import set_luigi_config

from .tools import create_not_empty_file


@pytest.fixture
def luigi_tools_params():
    """Simple luigi parameters for the TaskA task."""
    return {"TaskA": {"a_cfg": "default_value_in_cfg"}}


@pytest.fixture
def luigi_tools_working_directory(tmp_path, luigi_tools_params):
    """Set luigi config."""
    with set_luigi_config(luigi_tools_params):
        yield tmp_path


@pytest.fixture
def task_collection(tmpdir):
    """A collection of test classes."""

    class TaskClasses:
        """Class with some luigi tasks to test."""

        def __init__(self):
            self.tmpdir = tmpdir
            self.reset_classes()
            self.classes = self._classes()
            self.targets = self._targets()
            self.reset_classes()  # Reset again to return classes that are not registered by luigi

        def reset_classes(self):
            """Reset all test classes."""

            class TaskA(luigi_tools.task.WorkflowTask):
                """A simple test task."""

                counter = luigi.IntParameter(default=0)

                def run(self):
                    for i in luigi.task.flatten(self.output()):
                        create_not_empty_file(i.path)

                def output(self):
                    return luigi.LocalTarget(tmpdir / "TaskA.target")

            class TaskB(luigi_tools.task.WorkflowTask):
                """A simple test task."""

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
                """A simple test task."""

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
                """A simple test task."""

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
                """A simple test task."""

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
