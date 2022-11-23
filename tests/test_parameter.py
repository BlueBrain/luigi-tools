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

"""Tests for luigi-tools parameters."""

import collections
import dataclasses
import typing

# pylint: disable=empty-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
import warnings

import luigi
import mock
import pytest

import luigi_tools.parameter
import luigi_tools.target
import luigi_tools.task
import luigi_tools.util
from luigi_tools.util import set_luigi_config

from .tools import create_empty_file


def test_ext_parameter(luigi_tools_working_directory):
    """Test the `luigi_tools.parameter.ExtParameter` class."""

    class TaskExtParameter(luigi.Task):
        """A simple test task."""

        a = luigi_tools.parameter.ExtParameter(default=".ext_from_default")
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
    """Test the `luigi_tools.parameter.RatioParameter` class."""

    class TaskRatioParameter(luigi.Task):
        """A simple test task."""

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


class TestOptionalParameter:
    """Test the `luigi_tools.parameter.Optional*` classes.

    .. note::

        These classes were ported to the luigi package but the tests were kept here for example
        purpose.
    """

    def test_optional_parameter(self, luigi_tools_working_directory):
        """Simple test for optional parameters."""
        # pylint: disable=too-many-statements
        class TaskFactory:
            """A class factory."""

            def __call__(self):
                class TaskOptionalParameter(luigi.Task):
                    """A simple test task."""

                    a = luigi.parameter.OptionalIntParameter(default=1)
                    b = luigi.parameter.OptionalFloatParameter(default=1.5)
                    c = luigi.parameter.OptionalNumericalParameter(
                        default=0.75, min_value=0, max_value=1, var_type=float
                    )
                    d = luigi_tools.parameter.OptionalRatioParameter(default=0.5)
                    e = luigi.parameter.OptionalIntParameter(default=None)
                    f = luigi.parameter.OptionalListParameter(default=None)
                    g = luigi.parameter.OptionalChoiceParameter(default=None, choices=["a", "b"])
                    h = luigi.parameter.OptionalStrParameter(default="h")
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
                        # pylint: disable=consider-using-f-string
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

        class TaskOptionalParameterWarning(luigi.Task):
            """A simple test task."""

            a = luigi.parameter.OptionalIntParameter(default=1)
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
                category=luigi.parameter.OptionalParameterTypeWarning,
            )
            assert luigi.build(
                [TaskOptionalParameterWarning(a="zz", expected_a="zz")],
                local_scheduler=True,
            )

            assert len(w) == 1
            assert issubclass(w[0].category, luigi.parameter.OptionalParameterTypeWarning)
            assert str(w[0].message) == (
                """OptionalIntParameter "a" with value "zz" is not of type "int" or None."""
            )

    def test_warning(self):
        """Test that warnings are properly emitted."""

        class TestOptionalFloatParameterSingleType(
            luigi.parameter.OptionalParameterMixin, luigi.FloatParameter
        ):
            """Class to parse optional float parameters."""

            expected_type = float

        class TestOptionalFloatParameterMultiTypes(
            luigi.parameter.OptionalParameterMixin, luigi.FloatParameter
        ):
            """Class to parse optional float parameters."""

            expected_type = (int, float)

        class TestConfig(luigi.Config):
            """A simple config task."""

            param_single = TestOptionalFloatParameterSingleType()
            param_multi = TestOptionalFloatParameterMultiTypes()

        with warnings.catch_warnings(record=True) as record:
            TestConfig(param_single=0.0, param_multi=1.0)

        assert len(record) == 0

        with warnings.catch_warnings(record=True) as record:
            warnings.filterwarnings(
                action="ignore",
                category=Warning,
            )
            warnings.simplefilter(
                action="always",
                category=luigi.parameter.OptionalParameterTypeWarning,
            )
            assert luigi.build(
                [TestConfig(param_single="0", param_multi="1")], local_scheduler=True
            )

        assert len(record) == 2
        assert issubclass(record[0].category, luigi.parameter.OptionalParameterTypeWarning)
        assert issubclass(record[1].category, luigi.parameter.OptionalParameterTypeWarning)
        assert str(record[0].message) == (
            """TestOptionalFloatParameterSingleType "param_single" with value "0" is not of type """
            """"float" or None."""
        )
        assert str(record[1].message) == (
            """TestOptionalFloatParameterMultiTypes "param_multi" with value "1" is not of any """
            """type in ["int", "float"] or None."""
        )

    def actual_test(self, cls, default, expected_value, expected_type, bad_data, **kwargs):
        """A test function used for several parameter types."""

        class TestConfig(luigi.Config):
            """A simple config task."""

            param = cls(default=default, **kwargs)
            empty_param = cls(default=default, **kwargs)

            def run(self):
                assert self.param == expected_value
                assert self.empty_param is None

        # Test parsing empty string (should be None)
        assert cls(**kwargs).parse("") is None

        # Test that warning is raised only with bad type
        with mock.patch("luigi.parameter.warnings") as warnings:
            TestConfig()
            warnings.warn.assert_not_called()

        if cls != luigi.parameter.OptionalChoiceParameter:
            with mock.patch("luigi.parameter.warnings") as warnings:
                TestConfig(param=None)
                warnings.warn.assert_not_called()

            with mock.patch("luigi.parameter.warnings") as warnings:
                TestConfig(param=bad_data)
                if cls == luigi_tools.parameter.OptionalBoolParameter:
                    warnings.warn.assert_not_called()
                else:
                    assert warnings.warn.call_count == 1
                    # pylint: disable=consider-using-f-string
                    warnings.warn.assert_called_with(
                        """{} "param" with value "{}" is not of type "{}" or None.""".format(
                            cls.__name__, bad_data, expected_type
                        ),
                        luigi.parameter.OptionalParameterTypeWarning,
                    )

        # Test with value from config
        assert luigi.build([TestConfig()], local_scheduler=True)

    def test_optional_str_parameter(self, tmp_working_dir):
        """Test the `luigi.parameter.OptionalStrParameter` class."""
        with set_luigi_config({"TestConfig": {"param": "expected value", "empty_param": ""}}):
            self.actual_test(
                luigi.parameter.OptionalStrParameter,
                None,
                "expected value",
                "str",
                0,
            )
            self.actual_test(
                luigi.parameter.OptionalStrParameter,
                "default value",
                "expected value",
                "str",
                0,
            )

    def test_optional_int_parameter(self, tmp_working_dir):
        """Test the `luigi.parameter.OptionalIntParameter` class."""
        with set_luigi_config({"TestConfig": {"param": "10", "empty_param": ""}}):
            self.actual_test(luigi.parameter.OptionalIntParameter, None, 10, "int", "bad data")
            self.actual_test(luigi.parameter.OptionalIntParameter, 1, 10, "int", "bad data")

    def test_optional_bool_parameter(self, tmp_working_dir):
        """Test the `luigi.parameter.OptionalBoolParameter` class."""
        with set_luigi_config({"TestConfig": {"param": "true", "empty_param": ""}}):
            self.actual_test(
                luigi_tools.parameter.OptionalBoolParameter,
                None,
                True,
                "bool",
                "bad data",
            )
            self.actual_test(
                luigi_tools.parameter.OptionalBoolParameter,
                False,
                True,
                "bool",
                "bad data",
            )

    def test_optional_float_parameter(self, tmp_working_dir):
        """Test the `luigi.parameter.OptionalFloatParameter` class."""
        with set_luigi_config({"TestConfig": {"param": "10.5", "empty_param": ""}}):
            self.actual_test(
                luigi.parameter.OptionalFloatParameter,
                None,
                10.5,
                "float",
                "bad data",
            )
            self.actual_test(
                luigi.parameter.OptionalFloatParameter,
                1.5,
                10.5,
                "float",
                "bad data",
            )

    def test_optional_dict_parameter(self, tmp_working_dir):
        """Test the `luigi.parameter.OptionalDictParameter` class."""
        with set_luigi_config({"TestConfig": {"param": '{"a": 10}', "empty_param": ""}}):
            self.actual_test(
                luigi.parameter.OptionalDictParameter,
                None,
                {"a": 10},
                "FrozenOrderedDict",
                "bad data",
            )
            self.actual_test(
                luigi.parameter.OptionalDictParameter,
                {"a": 1},
                {"a": 10},
                "FrozenOrderedDict",
                "bad data",
            )

    def test_optional_list_parameter(self, tmp_working_dir):
        """Test the `luigi.parameter.OptionalListParameter` class."""
        with set_luigi_config({"TestConfig": {"param": "[10.5]", "empty_param": ""}}):
            self.actual_test(
                luigi.parameter.OptionalListParameter,
                None,
                (10.5,),
                "tuple",
                "bad data",
            )
            self.actual_test(
                luigi.parameter.OptionalListParameter,
                (1.5,),
                (10.5,),
                "tuple",
                "bad data",
            )

    def test_optional_tuple_parameter(self, tmp_working_dir):
        """Test the `luigi.parameter.OptionalTupleParameter` class."""
        with set_luigi_config({"TestConfig": {"param": "[10.5]", "empty_param": ""}}):
            self.actual_test(
                luigi.parameter.OptionalTupleParameter,
                None,
                (10.5,),
                "tuple",
                "bad data",
            )
            self.actual_test(
                luigi.parameter.OptionalTupleParameter,
                (1.5,),
                (10.5,),
                "tuple",
                "bad data",
            )

    def test_optional_numerical_parameter(self, tmp_working_dir):
        """Test the `luigi.parameter.OptionalNumericalParameter` class."""
        with set_luigi_config({"TestConfig": {"param": "10.5", "empty_param": ""}}):
            self.actual_test(
                luigi.parameter.OptionalNumericalParameter,
                None,
                10.5,
                "float",
                "bad data",
                var_type=float,
                min_value=0,
                max_value=100,
            )
            self.actual_test(
                luigi.parameter.OptionalNumericalParameter,
                1.5,
                10.5,
                "float",
                "bad data",
                var_type=float,
                min_value=0,
                max_value=100,
            )

    def test_optional_choice_parameter(self, tmp_working_dir):
        """Test the `luigi.parameter.OptionalChoiceParameter` class."""
        with set_luigi_config({"TestConfig": {"param": "expected value", "empty_param": ""}}):
            choices = ["default value", "expected value", "null"]
            self.actual_test(
                luigi.parameter.OptionalChoiceParameter,
                None,
                "expected value",
                "str",
                "bad data",
                choices=choices,
            )
            self.actual_test(
                luigi.parameter.OptionalChoiceParameter,
                "default value",
                "expected value",
                "str",
                "bad data",
                choices=choices,
            )


class TestBoolParameter:
    """Test the `luigi_tools.parameter.BoolParameter` class."""

    def test_auto_set_parsing(self):
        """Test that parsing is properly fixed according to the default value."""

        class TaskBoolParameter(luigi.Task):
            """A simple test task."""

            a = luigi_tools.parameter.BoolParameter(default=True)
            b = luigi_tools.parameter.BoolParameter(default=False)
            c = luigi_tools.parameter.BoolParameter(default=False, parsing="explicit")
            d = luigi_tools.parameter.BoolParameter()

            def run(self):
                assert self.a is True
                assert self.b is False
                assert self.c is False
                assert self.d is False and self.d is not None

            def output(self):
                return luigi.LocalTarget("not_existing_file")

        assert TaskBoolParameter.a.parsing == "explicit"
        assert TaskBoolParameter.b.parsing == "implicit"
        assert TaskBoolParameter.c.parsing == "explicit"

        with set_luigi_config():
            assert luigi.build([TaskBoolParameter()], local_scheduler=True)

    def test_true_implicit_failing(self):
        """Test that setting `default=True` and `parsing="implicit"` raises an exception."""
        # pylint: disable=unused-variable
        with pytest.raises(ValueError):

            class TaskBoolParameterFail(luigi.Task):
                """A simple test task."""

                a = luigi_tools.parameter.BoolParameter(default=True, parsing="implicit")


@pytest.mark.parametrize("default", [None, "not_existing_dir"])
@pytest.mark.parametrize("absolute", [True, False])
@pytest.mark.parametrize("exists", [True, False])
def test_path_parameter(tmpdir, default, absolute, exists):
    """Test the `luigi.parameter.PathParameter` class."""

    class TaskPathParameter(luigi.Task):
        """A simple test task."""

        a = luigi.parameter.PathParameter(
            default=str(tmpdir / default) if default is not None else str(tmpdir),
            absolute=absolute,
            exists=exists,
        )
        b = luigi.parameter.OptionalPathParameter(
            default=str(tmpdir / default) if default is not None else str(tmpdir),
            absolute=absolute,
            exists=exists,
        )
        c = luigi.parameter.OptionalPathParameter(default=None)
        d = luigi.parameter.OptionalPathParameter(default="not empty default")

        def run(self):
            # Use the parameter as a Path object
            new_file = self.a / "test.file"
            new_optional_file = self.b / "test_optional.file"
            if default is not None:
                new_file.parent.mkdir(parents=True)
            new_file.touch()
            new_optional_file.touch()
            assert new_file.exists()
            assert new_optional_file.exists()
            assert self.c is None
            assert self.d is None

        def output(self):
            return luigi.LocalTarget("not_existing_file")

    # Test with default values
    with set_luigi_config({"TaskPathParameter": {"d": ""}}):
        if default is not None and exists:
            with pytest.raises(ValueError, match="The path .* does not exist"):
                luigi.build([TaskPathParameter()], local_scheduler=True)
        else:
            assert luigi.build([TaskPathParameter()], local_scheduler=True)

    # Test with values from config
    with set_luigi_config(
        {
            "TaskPathParameter": {
                "a": str(tmpdir / (default + "_from_config"))
                if default is not None
                else str(tmpdir),
                "b": str(tmpdir / (default + "_from_config"))
                if default is not None
                else str(tmpdir),
                "d": "",
            }
        }
    ):
        if default is not None and exists:
            with pytest.raises(ValueError, match="The path .* does not exist"):
                luigi.build([TaskPathParameter()], local_scheduler=True)
        else:
            assert luigi.build([TaskPathParameter()], local_scheduler=True)


def test_DataclassParameter__primitives():
    @dataclasses.dataclass
    class MyClass:
        a: str
        b: int
        c: float
        d: bool

    instance = MyClass(a="1", b=2, c=3.0, d=False)

    p = luigi_tools.parameter.DataclassParameter(cls_type=MyClass)

    string = p.serialize(instance)
    assert string == '{"a": "1", "b": 2, "c": 3.0, "d": false}'

    deserialized_dict = p.parse(string)
    assert deserialized_dict == {"a": "1", "b": 2, "c": 3.0, "d": False}

    normalized_instance = p.normalize(instance)
    assert normalized_instance == instance

    normalized_instance = p.normalize(deserialized_dict)
    assert normalized_instance == instance


def test_DataclassParameter__primitives__ordering():
    @dataclasses.dataclass(frozen=True, eq=True)
    class A:
        a: str
        b: int
        c: float
        d: bool

    @dataclasses.dataclass(frozen=True, eq=True)
    class B:
        b: int
        a: str
        d: bool
        c: float

    a = A(a="1", b=2, c=3.0, d=False)
    b = B(a="1", b=2, c=3.0, d=False)

    pA = luigi_tools.parameter.DataclassParameter(cls_type=A)
    pB = luigi_tools.parameter.DataclassParameter(cls_type=B)

    # ordering affects serialization
    sA = pA.serialize(a)
    sB = pB.serialize(b)
    assert sA != sB

    # but not dictionary comparison
    dA = pA.parse(sA)
    dB = pB.parse(sB)
    assert dA == dB

    # ordering also affects deserialization and hashes
    nA = pA.normalize(a)
    nB = pB.normalize(b)
    assert nA != nB
    assert hash(nA) != hash(nB)

    # ordering also affects deserialization and hashes
    nA = pA.normalize(dA)
    nB = pB.normalize(dB)
    assert nA != nB
    assert hash(nA) != hash(nB)


def test_DataclassParameter__sequences():
    @dataclasses.dataclass(frozen=True, eq=True)
    class A:
        a: list
        b: tuple
        c: typing.List[str]
        d: typing.List[int]
        e: typing.Tuple[float]

    a = A(a=[1, "1"], b=(2.0, "2"), c=["a", "b"], d=[3, 4], e=(5.0, 6.0))

    p = luigi_tools.parameter.DataclassParameter(cls_type=A)

    # Note how the e tuple has been converted to a list with json serialization
    string = p.serialize(a)
    assert (
        string == '{"a": [1, "1"], "b": [2.0, "2"], "c": ["a", "b"], "d": [3, 4], "e": [5.0, 6.0]}'
    )

    # Note how the e tuple has been converted to a list because of the json serialization
    a_dict = p.parse(string)
    assert a_dict == {"a": [1, "1"], "b": [2.0, "2"], "c": ["a", "b"], "d": [3, 4], "e": [5.0, 6.0]}

    a_obj = p.normalize(a_dict)
    assert type(a_obj.a) == tuple and a_obj.a == tuple(a.a)
    assert type(a_obj.b) == tuple and a_obj.b == a.b
    assert type(a_obj.c) == tuple and a_obj.c == tuple(a.c)
    assert type(a_obj.d) == tuple and a_obj.d == tuple(a.d)
    assert type(a_obj.e) == tuple and a_obj.e == a.e

    a_obj = p.normalize(a)
    assert type(a_obj.a) == tuple and a_obj.a == tuple(a.a)
    assert type(a_obj.b) == tuple and a_obj.b == a.b
    assert type(a_obj.c) == tuple and a_obj.c == tuple(a.c)
    assert type(a_obj.d) == tuple and a_obj.d == tuple(a.d)
    assert type(a_obj.e) == tuple and a_obj.e == a.e


def test_DataclassParameter__mappings():
    @dataclasses.dataclass(frozen=True, eq=True)
    class A:
        a: dict
        b: collections.OrderedDict
        c: typing.Dict[str, int]
        d: typing.Dict[int, float]

    a = A(
        a={"a": 1, "b": "2"},
        b=collections.OrderedDict([("c", 3.0), ("d", 4)]),
        c={"e": 5, "f": 6},
        d={7: 8.0, 9: 10.0},
    )

    p = luigi_tools.parameter.DataclassParameter(cls_type=A)

    # Note how json serialization converts d keys into strings
    string = p.serialize(a)
    assert (
        string
        == '{"a": {"a": 1, "b": "2"}, "b": {"c": 3.0, "d": 4}, "c": {"e": 5, "f": 6}, "d": {"7": 8.0, "9": 10.0}}'
    )

    a_dict = p.parse(string)
    assert {
        "a": {"a": 1, "b": "2"},
        "b": {"c": 3.0, "d": 4},
        "c": {"e": 5, "f": 6},
        "d": {"7": 8.0, "9": 10.0},
    }

    for obj in (a_dict, a):
        a_obj = p.normalize(obj)
        for f in dataclasses.fields(a_obj):

            actual = getattr(a_obj, f.name)
            expected = getattr(a, f.name)

            # all dicts are converted to frozen ordered dicts
            assert isinstance(actual, luigi.freezing.FrozenOrderedDict)

            # check that dataclass is hashable
            assert hash(actual)
            assert dict(actual) == dict(expected)


def test_DataclassParameter__nesting():
    @dataclasses.dataclass(frozen=True, eq=True)
    class A:
        a: str
        b: float

    @dataclasses.dataclass(frozen=True, eq=True)
    class B:
        c: A
        d: float

    @dataclasses.dataclass(frozen=True, eq=True)
    class C:
        e: A
        f: B

    c = C(e=A(a="1", b=2.0), f=B(c=A(a="2", b=3.0), d=4.0))

    p = luigi_tools.parameter.DataclassParameter(cls_type=C)

    string = p.serialize(c)
    assert string == '{"e": {"a": "1", "b": 2.0}, "f": {"c": {"a": "2", "b": 3.0}, "d": 4.0}}'

    c_dict = p.parse(string)
    assert c_dict == {"e": {"a": "1", "b": 2.0}, "f": {"c": {"a": "2", "b": 3.0}, "d": 4.0}}

    c_obj = p.normalize(c_dict)
    assert c_obj == c

    c_obj = p.normalize(c)
    assert c_obj == c
