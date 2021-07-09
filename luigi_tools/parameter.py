"""This module provides some specific luigi parameters.

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
        **kwargs
    ):
        super().__init__(
            *args,
            min_value=0,
            max_value=1,
            var_type=float,
            left_op=left_op,
            right_op=right_op,
            **kwargs
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


class OptionalParameter(luigi.OptionalParameter):
    """Mixin to make a parameter class optional."""

    expected_type = type(None)

    def __init__(self, *args, **kwargs):
        self._cls = self.__class__
        self._base_cls = self.__class__.__bases__[-1]
        if OptionalParameter in (self._cls, self._base_cls):
            raise TypeError(
                "OptionalParameter can only be used as a mixin (must not be the rightmost "
                "class in the class definition)"
            )
        super().__init__(*args, **kwargs)

    def parse(self, x):
        """Parse the given value if it is not an empty string and not equal to ``'null'``."""
        if not isinstance(x, str):
            return x
        elif x and x.lower() != "null":
            return self._base_cls.parse(self, x)
        else:
            return None

    def normalize(self, x):
        """Normalize the given value if it is not ``None``."""
        if x is not None:
            return self._base_cls.normalize(self, x)
        else:
            return None

    def _warn_on_wrong_param_type(self, param_name, param_value):
        if self.__class__ != self._cls:  # pragma: no cover
            return
        if not isinstance(param_value, self._cls.expected_type) and param_value is not None:
            warnings.warn(
                '{} "{}" with value "{}" is not of type {} or None.'.format(
                    self._cls.__name__,
                    param_name,
                    param_value,
                    self._cls.expected_type.__name__,
                ),
                OptionalParameterTypeWarning,
            )


class OptionalStrParameter(OptionalParameter, luigi.Parameter):
    """Class to parse optional str parameters."""

    expected_type = str


class OptionalBoolParameter(OptionalParameter, luigi.BoolParameter):
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
