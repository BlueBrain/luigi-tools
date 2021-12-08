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
import warnings
from pathlib import Path

import luigi
import luigi_tools


class OptionalParameterTypeWarning(UserWarning):
    """Warning class for OptionalParameter with wrong type.

    .. moved_to_luigi::
        :previous_luigi_version: 3.0.3
    """


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


class PathParameter(luigi.Parameter):
    """Class to parse file path parameters.

    Args:
        absolute (bool): the given path is converted to an absolute path.
        create (bool): a folder is automatically created to the given path.
        exists (bool): raise a :class:`ValueError` if the path does not exist.

    .. moved_to_luigi::
        :previous_luigi_version: 3.0.3

        The moved version dropped the ``create`` parameter as it was confusing for users that may
        use it instead of using a target.
    """

    def __init__(self, *args, absolute=False, create=False, exists=False, **kwargs):
        super().__init__(*args, **kwargs)

        self.absolute = absolute
        self.create = create
        self.exists = exists

        luigi_tools.moved_to_luigi_warning(previous_luigi_version="3.0.3")

    def normalize(self, x):
        """Normalize the given value to a :class:`pathlib.Path` object."""
        path = Path(x)
        if self.absolute:
            path = path.absolute()
        if self.create:
            path.mkdir(parents=True, exist_ok=True)
        if self.exists and not path.exists():
            raise ValueError(f"The path {path} does not exist.")
        return path


class OptionalParameter:
    """Mixin to make a parameter class optional.

    .. moved_to_luigi::
        :previous_luigi_version: 3.0.3
    """

    expected_type = type(None)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        luigi_tools.moved_to_luigi_warning(previous_luigi_version="3.0.3")

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
            try:
                # pylint: disable=not-an-iterable
                param_type = f"any type in {[i.__name__ for i in self.expected_type]}"
            except TypeError:
                param_type = f"type '{self.expected_type.__name__}'"
            warnings.warn(
                (
                    f"{self.__class__.__name__} '{param_name}' with value "
                    f"'{param_value}' is not of {param_type} or None."
                ),
                OptionalParameterTypeWarning,
            )


class OptionalStrParameter(OptionalParameter, luigi.Parameter):
    """Class to parse optional str parameters.

    .. moved_to_luigi::
        :previous_luigi_version: 3.0.3
    """

    expected_type = str


class OptionalBoolParameter(OptionalParameter, BoolParameter):
    """Class to parse optional bool parameters.

    .. moved_to_luigi::
        :previous_luigi_version: 3.0.3
    """

    expected_type = bool


class OptionalIntParameter(OptionalParameter, luigi.IntParameter):
    """Class to parse optional int parameters.

    .. moved_to_luigi::
        :previous_luigi_version: 3.0.3
    """

    expected_type = int


class OptionalFloatParameter(OptionalParameter, luigi.FloatParameter):
    """Class to parse optional float parameters.

    .. moved_to_luigi::
        :previous_luigi_version: 3.0.3
    """

    expected_type = float


class OptionalNumericalParameter(OptionalParameter, luigi.NumericalParameter):
    """Class to parse optional numerical parameters.

    .. moved_to_luigi::
        :previous_luigi_version: 3.0.3
    """

    expected_type = float


class OptionalRatioParameter(OptionalParameter, RatioParameter):
    """Class to parse optional ratio parameters.

    .. moved_to_luigi::
        :previous_luigi_version: 3.0.3
    """

    expected_type = float


class OptionalChoiceParameter(OptionalParameter, luigi.ChoiceParameter):
    """Class to parse optional choice parameters.

    .. moved_to_luigi::
        :previous_luigi_version: 3.0.3
    """

    expected_type = str


class OptionalListParameter(OptionalParameter, luigi.ListParameter):
    """Class to parse optional list parameters.

    .. moved_to_luigi::
        :previous_luigi_version: 3.0.3
    """

    expected_type = tuple


class OptionalDictParameter(OptionalParameter, luigi.DictParameter):
    """Class to parse optional dict parameters.

    .. moved_to_luigi::
        :previous_luigi_version: 3.0.3
    """

    expected_type = luigi.freezing.FrozenOrderedDict


class OptionalTupleParameter(OptionalParameter, luigi.TupleParameter):
    """Class to parse optional tuple parameters.

    .. moved_to_luigi::
        :previous_luigi_version: 3.0.3
    """

    expected_type = tuple


class OptionalPathParameter(OptionalParameter, PathParameter):
    """Class to parse optional path parameters.

    .. moved_to_luigi::
        :previous_luigi_version: 3.0.3
    """

    expected_type = (str, Path)
