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

"""This module provides some specific luigi parameters."""
import collections.abc
import dataclasses
import typing

import luigi
import typing_extensions
from luigi.freezing import FrozenOrderedDict
from luigi.parameter import OptionalParameterMixin


class ExtParameter(luigi.Parameter):
    """Class to parse file extension parameters."""

    def normalize(self, x):
        """Normalize the given value to an extension suffix without dot."""
        if x.startswith("."):
            x = x[1:]
        return x


class RatioParameter(luigi.NumericalParameter):
    """Class to parse ratio parameters.

    The argument must be a float between 0 and 1.
    The operators to include or exclude the boundaries can be set with 'left_op' and
    'right_op' parameters. Defaults operators include the boundaries.
    """

    def __init__(
        self,
        *args,
        left_op=luigi.parameter.operator.le,
        right_op=luigi.parameter.operator.le,
        **kwargs,
    ):
        super().__init__(
            *args,
            min_value=0,
            max_value=1,
            var_type=float,
            left_op=left_op,
            right_op=right_op,
            **kwargs,
        )

    def normalize(self, x):
        """Ensure that the value is contained in the given interval.

        The ``parse()`` method is not called when the parameters are given to the constructor,
        but the value check is only done there. This ``normalize()`` method ensures that the
        interval check is made even when for arguments given to the constructor.
        """
        if x is not None:
            return super().parse(x)
        else:
            return x


class BoolParameter(luigi.BoolParameter):
    """Class to parse boolean parameters and set explicit parsing when default is True."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self._default is True:
            if kwargs.get("parsing", "explicit") == self.__class__.IMPLICIT_PARSING:
                raise ValueError(
                    "A BoolParameter with 'default = True' can not use implicit parsing."
                )
            self.parsing = self.__class__.EXPLICIT_PARSING


class OptionalBoolParameter(OptionalParameterMixin, BoolParameter):
    """Class to parse optional bool parameters."""

    expected_type = bool


class OptionalRatioParameter(OptionalParameterMixin, RatioParameter):
    """Class to parse optional ratio parameters."""

    expected_type = float


class DataclassParameter(luigi.DictParameter):
    """Class to parse, serialize, and normalize nested dataclasses.

    Note: Similar to DictParameter, json serialization transforms int keys of dicts into str. The
    dataclass however transforms the keys into their correct type if the respective annotation is
    available.
    """

    def __init__(self, cls_type, *args, **kwargs):
        if not _is_dataclass(cls_type):
            raise TypeError(f"Class type {cls_type.__name__!r} is not a dataclass.")

        self._cls_type = cls_type
        super().__init__(*args, **kwargs)

    def normalize(self, value):
        """Normalize the given value to a dataclass initialized object."""
        if isinstance(value, self._cls_type):
            value = dataclasses.asdict(value)
        return _instantiate(self._cls_type, value)

    def serialize(self, x):
        """Serialize a dataclass object."""
        data = dataclasses.asdict(x)
        return super().serialize(data)


def _instantiate(cls, data):
    # pylint: disable=too-many-return-statements

    if data is None:
        return None

    if cls in {str, int, float, bool}:
        return cls(data)

    if _is_optional(cls):
        return _instantiate(typing_extensions.get_args(cls)[0], data)

    if _is_dataclass(cls):
        return _instantiate_dataclass(cls, data, _instantiate)

    if _is_sequence(data):
        return _instantiate_sequence(cls, data, _instantiate)

    if _is_mapping(data):
        return _instantiate_mapping(cls, data, _instantiate)

    return data


def _is_dataclass(cls):
    return isinstance(cls, type) and dataclasses.is_dataclass(cls)


def _instantiate_dataclass(cls, data, func):
    """Instantiate a dataclass from the given data using the instantiation func for recursion."""
    args = {f.name: func(f.type, data[f.name]) for f in dataclasses.fields(cls)}
    return cls(**args)


def _is_sequence(data):
    return isinstance(data, collections.abc.Sequence) and not isinstance(data, str)


def _is_optional(cls):
    """Optional types have Union as origin and args of the form [Type, NoneType].

    In that case the arguments of the Type are returned, if any.
    """
    return typing_extensions.get_origin(cls) is typing.Union and type(
        None
    ) in typing_extensions.get_args(cls)


def _get_type_args(cls):
    return typing_extensions.get_args(cls) or []


def _instantiate_sequence(cls, data, func):
    args = _get_type_args(cls)

    # Use type annotation to cast the sequence values
    if args:
        return tuple(func(args[0], v) for v in data)

    # Otherwise the types from the data
    return tuple(func(type(v), v) for v in data)


def _is_mapping(data):
    return isinstance(data, collections.abc.Mapping)


def _instantiate_mapping(cls, data, func):
    args = _get_type_args(cls)

    # Use key, value type annotations for casting
    if args:
        assert len(args) == 2, args
        arguments_generator = (
            (
                func(args[0], k),
                func(args[1], v),
            )
            for k, v in data.items()
        )
    # Otherwise use the types from the data
    else:
        arguments_generator = (
            (
                func(type(k), k),
                func(type(v), v),
            )
            for k, v in data.items()
        )
    return FrozenOrderedDict(arguments_generator)
