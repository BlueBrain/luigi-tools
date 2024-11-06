# Copyright 2021 Blue Brain Project / EPFL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The luigi-tools package.

Extend and add new features to the luigi package.
"""

import importlib.metadata
import warnings

__version__ = importlib.metadata.version("luigi-tools")


class MovedToLuigiWarning(DeprecationWarning):
    """Warning for features moved to the official luigi package."""


def moved_to_luigi_warning(
    luigi_version=None, previous_luigi_version=None, deprecation_version=None
):
    """Raise a deprecation warning for features moved to official luigi package.

    Args:
        luigi_version (str): The version of the luigi package from which the feature is available.
        previous_luigi_version (str): The version of the ``luigi`` package after which the feature
            is available (use this when the feature is merged into the master branch of the
            ``luigi`` package but it is not released yet).
        deprecation_version (str): The version of the ``luigi_tools`` package from which the feature
            will be removed.
    """
    if (luigi_version is None) == (previous_luigi_version is None):
        raise ValueError(
            "Either the 'luigi_version' or the 'previous_luigi_version' argument must be not None "
            "but not both of them"
        )
    if luigi_version is not None:
        msg = (
            "This feature was moved to the luigi package and is available since version "
            f"{luigi_version}."
        )
    else:
        msg = (
            "This feature was moved to the luigi package and will be available after version "
            f"{previous_luigi_version}."
        )

    if deprecation_version is not None:
        msg += f" It will be deprecated in version {deprecation_version}."
    warnings.warn(
        msg,
        MovedToLuigiWarning,
    )
