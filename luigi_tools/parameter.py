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
import re
import warnings

import luigi


class OptionalParameterTypeWarning(UserWarning):
    """Warning class for OptionalParameter with wrong type."""


class ExtParameter(luigi.Parameter):
    """Class to parse file extension parameters."""

    def parse(self, x):
        """Parse the given value to an extension suffix without dot."""
        pattern = re.compile(r"\.?(.*)")
        match = re.match(pattern, x)
        return match.group(1)


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


class OptionalParameter:
    """Mixin to make a parameter class optional."""

    expected_type = type(None)

    def serialize(self, x):
        """Parse the given value if the value is not None else return an empty string."""
        if x is None:
            return ""
        else:
            return super().serialize(x)  # pylint: disable=no-member

    def parse(self, x):
        """Parse the given value if it is not an empty string and not equal to ``'null'``."""
        if not isinstance(x, str):
            return x
        elif x:
            return super().parse(x)  # pylint: disable=no-member
        else:
            return None

    def normalize(self, x):
        """Normalize the given value if it is not ``None``."""
        if x is None:
            return None
        return super().normalize(x)  # pylint: disable=no-member

    def _warn_on_wrong_param_type(self, param_name, param_value):
        if not isinstance(param_value, self.expected_type) and param_value is not None:
            warnings.warn(
                (
                    f'{self.__class__.__name__} "{param_name}" with value '
                    f'"{param_value}" is not of type {self.expected_type.__name__} or None.'
                ),
                OptionalParameterTypeWarning,
            )


class OptionalStrParameter(OptionalParameter, luigi.Parameter):
    """Class to parse optional str parameters."""

    expected_type = str


class OptionalBoolParameter(OptionalParameter, BoolParameter):
    """Class to parse optional bool parameters."""

    expected_type = bool


class OptionalIntParameter(OptionalParameter, luigi.IntParameter):
    """Class to parse optional int parameters."""

    expected_type = int


class OptionalFloatParameter(OptionalParameter, luigi.FloatParameter):
    """Class to parse optional float parameters."""

    expected_type = float


class OptionalNumericalParameter(OptionalParameter, luigi.NumericalParameter):
    """Class to parse optional numerical parameters."""

    expected_type = float


class OptionalRatioParameter(OptionalParameter, RatioParameter):
    """Class to parse optional ratio parameters."""

    expected_type = float


class OptionalChoiceParameter(OptionalParameter, luigi.ChoiceParameter):
    """Class to parse optional choice parameters."""

    expected_type = str


class OptionalListParameter(OptionalParameter, luigi.ListParameter):
    """Class to parse optional list parameters."""

    expected_type = tuple


class OptionalDictParameter(OptionalParameter, luigi.DictParameter):
    """Class to parse optional dict parameters."""

    expected_type = luigi.freezing.FrozenOrderedDict


class OptionalTupleParameter(OptionalParameter, luigi.TupleParameter):
    """Class to parse optional tuple parameters."""

    expected_type = tuple
