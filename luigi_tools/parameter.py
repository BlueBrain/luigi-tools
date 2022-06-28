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

import luigi
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
