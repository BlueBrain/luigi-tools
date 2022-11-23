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
import inspect
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
    """Class to parse, serialize, and normalize nested dataclasses."""

    def __init__(self, cls_type, *args, **kwargs):
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

    origin_type = typing_extensions.get_origin(cls)

    if origin_type:  # typing type

        if inspect.isclass(origin_type):

            if origin_type in {tuple, list}:
                return _instantiate(origin_type, data)

            if issubclass(origin_type, collections.abc.Mapping):
                k_type, v_type = typing_extensions.get_args(cls)
                return FrozenOrderedDict(
                    (
                        (
                            _instantiate(k_type, k),
                            _instantiate(v_type, v),
                        )
                        for k, v in data.items()
                    )
                )

           raise TypeError(f"Unsupported type {cls}")

        if origin_type is typing.Union:
            args = typing_extensions.get_args(cls)
            if type(None) in args:  # optional
                return _instantiate(args[0], data) if data else None
            raise TypeError(f"Unsupported type {cls}")

        raise TypeError(f"Unsupported type {cls}")

    if cls is typing.Any:
        return _instantiate(type(data), data)

    if dataclasses.is_dataclass(cls):
        args = {f.name: _instantiate(f.type, data[f.name]) for f in dataclasses.fields(cls)}
        return cls(**args)

    if cls in {tuple, list}:
        return tuple(_instantiate(type(v), v) for v in data)

    if issubclass(cls, collections.abc.Mapping):
        return FrozenOrderedDict(
            (
                _instantiate(type(k), k),
                _instantiate(type(v), v),
            )
            for k, v in data.items()
        )

    return cls(data)
