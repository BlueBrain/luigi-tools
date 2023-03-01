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

"""This module provides some specific luigi tasks and associated tools."""
import logging
import types
from copy import deepcopy

import luigi

from luigi_tools.util import apply_over_outputs
from luigi_tools.util import apply_over_required
from luigi_tools.util import recursive_check
from luigi_tools.util import target_remove

L = logging.getLogger(__name__)


_no_default_value = "__no_default_value__"


class DuplicatedParameterError(Exception):
    """Exception raised when a parameter is duplicated in a task."""


class GlobalParameterNoValueError(Exception):
    """Exception raised when the value of a global parameter can not be found."""


class RerunMixin:
    """Mixin used to force a task to run again by setting the 'rerun' parameter to True."""

    rerun = luigi.BoolParameter(
        significant=False,
        default=False,
        description="Trigger to force the task to rerun.",
    )  #:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if recursive_check(self):
            apply_over_outputs(self, target_remove)

        if isinstance(self, luigi.WrapperTask) and self.rerun:

            def recursive_target_remove(task, *args, **kwargs):
                apply_over_outputs(task, target_remove)
                apply_over_required(task, recursive_target_remove)

            recursive_target_remove(self)


class LogTargetMixin:
    """Mixin used to log output paths of this task.

    .. note::
        To have an effect, this mixin must be placed to the left of the first class
        inheriting from :class:`luigi.Task` in the base class list.
    """

    def __init__(self, *args, **kwargs):
        event_handler = super().event_handler

        # pylint: disable=unused-variable
        @event_handler(luigi.Event.SUCCESS)
        def log_targets(task):
            """Hook to log output targets of a task."""

            def log_func(target, key=None):
                if not hasattr(target, "path"):
                    return
                class_name = task.__class__.__name__
                if key is None:
                    L.debug("Output of %s task: %s", class_name, target.path)
                else:
                    L.debug("Output %s of %s task: %s", key, class_name, target.path)

            apply_over_outputs(task, log_func)

        super().__init__(*args, **kwargs)


class GlobalParamMixin:
    """Mixin used to add customisable global parameters.

    For the tasks that inherit from this GlobalParamMixin, when a parameter is linked to
    another one and it value is None, its value is automatically replaced by the value
    of the linked parameter.
    See :class:`copy_params` for details about parameter linking.
    """

    def __init__(self, *args, **kwargs):
        event_handler = super().event_handler

        # pylint: disable=unused-variable
        @event_handler(luigi.Event.START)
        def log_parameters(self):
            """Hook to log actual parameter values considering their global processing."""
            class_name = self.__class__.__name__
            L.debug("Attributes of %s task after global processing:", class_name)
            for name in self.get_param_names():
                try:
                    L.debug("Attribute: %s == %s", name, getattr(self, name))
                except Exception:  # pylint: disable=broad-except; # pragma: no cover
                    L.debug("Can't print '%s' attribute for unknown reason", name)

        super().__init__(*args, **kwargs)

    def __getattribute__(self, name):
        """Get the attribute value and get it from the linked parameter is the value is None."""
        tmp = super().__getattribute__(name)
        if not isinstance(tmp, str) or tmp != _no_default_value:
            return tmp
        if hasattr(self, "_global_params"):
            global_param = self._global_params.get(name)
            if global_param is not None:
                return getattr(global_param.cls(), global_param.name)
        raise GlobalParameterNoValueError(
            f"The value of the '{name}' parameter can not be retrieved from the linked parameter."
        )

    def __setattr__(self, name, value):
        """Send a warning logger entry when a global parameter is set to None."""
        try:
            global_params = self._global_params
        except AttributeError:
            global_params = {}
        if name in global_params and isinstance(value, str) and value == _no_default_value:
            L.warning(
                "The Parameter '%s' of the task '%s' is set to None, thus the global "
                "value will be taken frow now on",
                name,
                self.__class__.__name__,
            )
        return super().__setattr__(name, value)


class RemoveCorruptedOutputMixin:
    """Mixin used to remove incomplete outputs of a failed Task.

    The clean_failed parameter must be set True to enable this feature.
    """

    clean_failed = luigi.BoolParameter(
        significant=False,
        default=False,
        description="Trigger to remove the outputs of the failed tasks.",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        event_handler = super().event_handler

        # pylint: disable=unused-variable, unused-argument
        @event_handler(luigi.Event.FAILURE)
        def remove_all_output(self, exception):
            class_name = self.__class__.__name__
            if self.clean_failed:
                L.debug("%s failed! Cleaning the targets...", class_name)
                apply_over_outputs(self, target_remove)


class ParamRef:
    """Class to store parameter reference information."""

    def __init__(self, cls, name=None, default=_no_default_value):
        self.cls = cls
        self.name = name
        self.default = default


class copy_params:
    """Copy a parameter from another Task.

    This decorator takes kwargs where keys are the parameter names and the values are
    :class:`ParamRef` instances.

    If no default value is given to the :class:`ParamRef`, two behaviours are possible:

        * If the task inherits from the :class:`GlobalParamMixin`, the parameter is linked to the
          one of the base class. The default parameter value is then the actual value of the
          linked parameter.
        * If the task does not inherit from the :class:`GlobalParamMixin`, the default value is
          copied from the linked parameter.

    Differences with :class:`luigi.util.inherits`:

        * :class:`luigi.util.inherits` set all other class' parameters that the current one is
          missing. It is not possible to select a subset of parameters.
        * :class:`luigi.util.inherits` set the same value to the other class' parameter than the
          current one's parameter. This class does not set the same value, except for global
          parameters with no given value.

    **Usage**:

    .. code-block:: python

        class AnotherTask(luigi.Task):
            m = luigi.IntParameter(default=1)

        @copy_params(m=ParamRef(AnotherTask))
        class MyFirstTask(luigi.Task):
            def run(self):
               print(self.m) # this will be defined and print 1
               # ...

        @copy_params(another_m=ParamRef(AnotherTask, "m"))
        class MySecondTask(luigi.Task):
            def run(self):
               print(self.another_m) # this will be defined and print 1
               # ...

        @copy_params(another_m=ParamRef(AnotherTask, "m", 5))
        class MyFirstTask(luigi.Task):
            def run(self):
               print(self.another_m) # this will be defined and print 5
               # ...

        @copy_params(another_m=ParamRef(AnotherTask, "m"))
        class MyFirstTask(GlobalParamMixin, luigi.Task):
            def run(self):
               # this will be defined and print 1 if self.another_m is not explicitly set
               # (this means that self.another_m == luigi_tools.tasks._no_default_value)
               print(self.another_m)
               # ...

    .. warning::
        There is a limitation of using :class:`copy_params` on a :class:`GlobalParamMixin`.
        In this particular case, the task from which a parameter is copied must be called
        before the inheriting task (which does not mean it must be executed before, just
        ``task()`` is enough). This is due to the metaclass magic of luigi.
    """

    def __init__(self, **params_to_copy):
        """Initialize the decorator."""
        super().__init__()
        if not params_to_copy:
            raise ValueError("params_to_copy cannot be empty")

        self.params_to_copy = params_to_copy

    def __call__(self, task_that_inherits):
        """Set and configure the attributes."""
        # Get all parameters
        for param_name, attr in self.params_to_copy.items():
            # Check if the parameter exists in the inheriting task
            if not hasattr(task_that_inherits, param_name):
                if attr.name is None:
                    attr.name = param_name

                # Copy param
                param = getattr(attr.cls, attr.name)
                new_param = deepcopy(param)

                # Set default value if required
                if attr.default != _no_default_value:
                    new_param._default = attr.default
                elif issubclass(task_that_inherits, GlobalParamMixin):
                    new_param._default = _no_default_value
                elif param._default == luigi.parameter._no_value:
                    # The deepcopy make the new_param._default != luigi.parameter._no_value
                    # so we must reset it in this case
                    new_param._default = luigi.parameter._no_value

                # Add it to the inheriting task with new default values
                setattr(task_that_inherits, param_name, new_param)

                # Do not emit warning because of wrong type
                def _warn_on_wrong_global_param_type(_self, global_param_name, global_param_value):
                    if global_param_value != _no_default_value:
                        # pylint: disable=protected-access
                        _self._actual_warn_on_wrong_param_type(
                            global_param_name, global_param_value
                        )

                new_param._actual_warn_on_wrong_param_type = new_param._warn_on_wrong_param_type
                new_param._warn_on_wrong_param_type = types.MethodType(
                    _warn_on_wrong_global_param_type, new_param
                )

                # Fix normalize() to not normalize when the parameter does not have default value
                def normalize(_self, value):
                    # pylint: disable=protected-access
                    if value == _no_default_value:
                        return value
                    else:
                        return _self._actual_normalize(value)

                new_param._actual_normalize = new_param.normalize
                new_param.normalize = types.MethodType(normalize, new_param)

                # Add link to global parameter
                if issubclass(task_that_inherits, GlobalParamMixin):
                    if not hasattr(task_that_inherits, "_global_params"):
                        task_that_inherits._global_params = {}
                    task_that_inherits._global_params[param_name] = attr
            else:
                raise DuplicatedParameterError(
                    f"The parameter '{param_name}' already exists in the task "
                    f"'{task_that_inherits.__class__.__name__}'."
                )

        return task_that_inherits


class WorkflowTask(GlobalParamMixin, RerunMixin, luigi.Task):
    """Default task used in workflows.

    This task can be forced running again by setting the 'rerun' parameter to True.
    It can also use copy and link parameters from other tasks.
    """


class WorkflowWrapperTask(WorkflowTask, luigi.WrapperTask):
    """Base wrapper class with global parameters."""
