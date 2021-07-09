"""Tests for luigi-tools parameters.

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
import warnings

import luigi
import pytest

import luigi_tools.parameter
import luigi_tools.task
import luigi_tools.target
import luigi_tools.util
from luigi_tools.util import set_luigi_config

from .tools import create_empty_file


def test_ext_parameter(luigi_tools_working_directory):
    class TaskExtParameter(luigi.Task):
        """"""

        a = luigi_tools.parameter.ExtParameter(default="ext_from_default")
        test_type = luigi.Parameter(default="default")

        def run(self):
            if self.test_type == "default":
                assert self.a == "ext_from_default"
            else:
                assert self.a == "ext_from_cfg"
            create_empty_file(self.output().path)

        def output(self):
            return luigi.LocalTarget(
                luigi_tools_working_directory / f"test_ext_parameter_{self.a}_{self.test_type}.test"
            )

    assert luigi.build([TaskExtParameter(test_type="default")], local_scheduler=True)

    with set_luigi_config({"TaskExtParameter": {"a": "ext_from_cfg"}}):
        assert luigi.build([TaskExtParameter(test_type="cfg")], local_scheduler=True)

    with set_luigi_config({"TaskExtParameter": {"a": ".ext_from_cfg"}}):
        assert luigi.build([TaskExtParameter(test_type="cfg_with_dot")], local_scheduler=True)


def test_ratio_parameter(tmpdir):
    class TaskRatioParameter(luigi.Task):
        """"""

        a = luigi_tools.parameter.RatioParameter(default=0.5)
        b = luigi.FloatParameter(default=0.5)

        def run(self):
            assert self.a == self.b
            create_empty_file(self.output().path)

        def output(self):
            return luigi.LocalTarget(tmpdir / f"test_ratio_parameter_{self.a}_{self.b}.test")

    assert luigi.build([TaskRatioParameter()], local_scheduler=True)
    assert luigi.build([TaskRatioParameter(a=0.25, b=0.25)], local_scheduler=True)
    assert luigi.build([TaskRatioParameter(a=0, b=0)], local_scheduler=True)
    assert luigi.build([TaskRatioParameter(a=1, b=1)], local_scheduler=True)
    with pytest.raises(ValueError):
        assert luigi.build([TaskRatioParameter(a=-1, b=-1)], local_scheduler=True)
    with pytest.raises(ValueError):
        assert luigi.build([TaskRatioParameter(a=2, b=2)], local_scheduler=True)
    with pytest.raises(ValueError):
        assert luigi.build([TaskRatioParameter(a="a", b="a")], local_scheduler=True)


def test_optional_parameter(luigi_tools_working_directory):
    class TaskFactory:
        def __call__(self):
            class TaskOptionalParameter(luigi.Task):
                """"""

                a = luigi_tools.parameter.OptionalIntParameter(default=1)
                b = luigi_tools.parameter.OptionalFloatParameter(default=1.5)
                c = luigi_tools.parameter.OptionalNumericalParameter(
                    default=0.75, min_value=0, max_value=1, var_type=float
                )
                d = luigi_tools.parameter.OptionalRatioParameter(default=0.5)
                e = luigi_tools.parameter.OptionalIntParameter(default=None)
                f = luigi_tools.parameter.OptionalListParameter(default=None)
                g = luigi_tools.parameter.OptionalChoiceParameter(default=None, choices=["a", "b"])
                h = luigi_tools.parameter.OptionalStrParameter(default="h")
                i = luigi_tools.parameter.OptionalBoolParameter(default=None)
                expected_a = luigi.IntParameter(default=1)
                expected_b = luigi.FloatParameter(default=1.5)
                expected_c = luigi.NumericalParameter(
                    default=0.75, min_value=0, max_value=1, var_type=float
                )
                expected_d = luigi_tools.parameter.RatioParameter(default=0.5)
                expected_e = luigi.IntParameter(default=None)
                expected_f = luigi.ListParameter(default=None)
                expected_g = luigi.ChoiceParameter(default="null", choices=["a", "b", "null"])
                expected_h = luigi.Parameter(default="h")
                expected_i = luigi.BoolParameter(default=None)

                def run(self):
                    if self.expected_g == "null":
                        self.expected_g = None
                    print(
                        "self.a =",
                        self.a,
                        "self.b =",
                        self.b,
                        "self.c =",
                        self.c,
                        "self.d =",
                        self.d,
                        "self.e =",
                        self.e,
                        "self.f =",
                        self.f,
                        "self.g =",
                        self.g,
                        "self.h =",
                        self.h,
                        "self.i =",
                        self.i,
                    )
                    assert self.a == self.expected_a
                    assert self.b == self.expected_b
                    assert self.c == self.expected_c
                    assert self.d == self.expected_d
                    assert self.e == self.expected_e
                    assert self.f == self.expected_f
                    assert self.g == self.expected_g
                    assert self.h == self.expected_h
                    assert self.i == self.expected_i
                    create_empty_file(self.output().path)

                def output(self):
                    return luigi.LocalTarget(
                        luigi_tools_working_directory
                        / (
                            "test_optional_parameter_"
                            "{}_{}_{}_{}_{}_{}_{}_{}_{}.test".format(
                                self.a,
                                self.b,
                                self.c,
                                self.d,
                                self.e,
                                self.f,
                                self.g,
                                self.h,
                                self.i,
                            )
                        )
                    )

            return TaskOptionalParameter

    factory = TaskFactory()

    task = factory()
    assert luigi.build([task()], local_scheduler=True)

    with set_luigi_config(
        {
            "TaskOptionalParameter": {
                "a": "",
                "b": "",
                "c": "",
                "d": "",
                "e": "",
                "f": "",
                "g": "",
                "h": "",
                "i": "",
            }
        }
    ):
        task = factory()
        assert luigi.build(
            [
                task(
                    expected_a=None,
                    expected_b=None,
                    expected_c=None,
                    expected_d=None,
                    expected_e=None,
                    expected_f=None,
                    expected_g="null",
                    expected_h=None,
                    expected_i=None,
                )
            ],
            local_scheduler=True,
        )

    with set_luigi_config(
        {
            "TaskOptionalParameter": {
                "a": "null",
                "b": "null",
                "c": "null",
                "d": "null",
                "e": "null",
                "f": "null",
                "g": "null",
                "h": "null",
                "i": "null",
            }
        }
    ):
        task = factory()
        assert luigi.build(
            [
                task(
                    expected_a=None,
                    expected_b=None,
                    expected_c=None,
                    expected_d=None,
                    expected_e=None,
                    expected_f=None,
                    expected_g="null",
                    expected_h=None,
                    expected_i=None,
                )
            ],
            local_scheduler=True,
        )

    with set_luigi_config(
        {
            "TaskOptionalParameter": {
                "a": "0",
                "b": "0.7",
                "c": "0.8",
                "d": "0.9",
                "e": "1",
                "f": "[1, 2]",
                "g": "b",
                "h": "b",
                "i": "true",
            }
        }
    ):
        task = factory()
        assert luigi.build(
            [
                task(
                    expected_a=0,
                    expected_b=0.7,
                    expected_c=0.8,
                    expected_d=0.9,
                    expected_e=1,
                    expected_f=[1, 2],
                    expected_g="b",
                    expected_h="b",
                    expected_i=True,
                )
            ],
            local_scheduler=True,
        )

    with set_luigi_config(
        {
            "TaskOptionalParameter": {
                "a": "not numerical",
            }
        }
    ):
        task = factory()
        with pytest.raises(ValueError):
            assert luigi.build([task()], local_scheduler=True)

    with set_luigi_config(
        {
            "TaskOptionalParameter": {
                "b": "not numerical",
            }
        }
    ):
        task = factory()
        with pytest.raises(ValueError):
            assert luigi.build([task()], local_scheduler=True)

    with set_luigi_config(
        {
            "TaskOptionalParameter": {
                "c": "not numerical",
            }
        }
    ):
        task = factory()
        with pytest.raises(ValueError):
            assert luigi.build([task()], local_scheduler=True)

    with set_luigi_config(
        {
            "TaskOptionalParameter": {
                "d": "not numerical",
            }
        }
    ):
        task = factory()
        with pytest.raises(ValueError):
            assert luigi.build([task()], local_scheduler=True)

    with set_luigi_config(
        {
            "TaskOptionalParameter": {
                "f": "not list",
            }
        }
    ):
        task = factory()
        with pytest.raises(ValueError):
            assert luigi.build([task()], local_scheduler=True)

    with set_luigi_config(
        {
            "TaskOptionalParameter": {
                "g": "not dict",
            }
        }
    ):
        task = factory()
        with pytest.raises(ValueError):
            assert luigi.build([task()], local_scheduler=True)

    with set_luigi_config(
        {
            "TaskOptionalParameter": {
                "i": "not bool",
            }
        }
    ):
        task = factory()
        with pytest.raises(ValueError):
            assert luigi.build([task()], local_scheduler=True)

    with set_luigi_config():

        with pytest.raises(TypeError):

            class TaskOptionalParameterFail(luigi.Task):
                a = luigi_tools.parameter.OptionalParameter()

    class TaskOptionalParameterWarning(luigi.Task):
        """"""

        a = luigi_tools.parameter.OptionalIntParameter(default=1)
        expected_a = luigi.IntParameter(default=1)

        def run(self):
            print("self.a =", self.a)
            assert self.a == self.expected_a
            create_empty_file(self.output().path)

        def output(self):
            return luigi.LocalTarget(
                luigi_tools_working_directory / f"test_optional_parameter_warning_{self.a}.test"
            )

    with warnings.catch_warnings(record=True) as w:
        warnings.filterwarnings(
            action="ignore",
            category=Warning,
        )
        warnings.simplefilter(
            action="always",
            category=luigi_tools.parameter.OptionalParameterTypeWarning,
        )
        assert luigi.build(
            [TaskOptionalParameterWarning(a="zz", expected_a="zz")],
            local_scheduler=True,
        )

        assert len(w) == 1
        assert issubclass(w[0].category, luigi_tools.parameter.OptionalParameterTypeWarning)
        assert str(w[0].message) == (
            'OptionalIntParameter "a" with value "zz" is not of type int or None.'
        )


def test_bool_parameter(luigi_tools_working_directory):
    class TaskBoolParameter(luigi.Task):
        """"""

        a = luigi_tools.parameter.BoolParameter(default=True)
        b = luigi_tools.parameter.BoolParameter(default=False)
        c = luigi_tools.parameter.BoolParameter(default=False, parsing="explicit")

        def run(self):
            create_empty_file(self.output().path)

        def output(self):
            return luigi.LocalTarget(
                luigi_tools_working_directory
                / f"test_bool_parameter_{self.a}_{self.b}_{self.c}.test"
            )

    assert TaskBoolParameter.a.parsing == "explicit"
    assert TaskBoolParameter.b.parsing == "implicit"
    assert TaskBoolParameter.c.parsing == "explicit"

    with pytest.raises(ValueError):

        class TaskBoolParameterFail(luigi.Task):
            """"""

            a = luigi_tools.parameter.BoolParameter(default=True, parsing="implicit")
