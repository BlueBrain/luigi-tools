"""This module provides some fonctions to work with luigi tasks."""
import logging

import luigi
from luigi.parameter import _no_value as PARAM_NO_VALUE


L = logging.getLogger(__name__)


class WorkflowError(Exception):
    """Exception raised when the workflow is not consistent."""


def recursive_check(task, attr="rerun"):
    """Check if a task or any of its recursive dependencies has a given attribute set to True."""
    val = getattr(task, attr, False)

    for dep in task.deps():
        val = val or getattr(dep, attr, False) or recursive_check(dep, attr)

    return val


def target_remove(target, *args, **kwargs):
    """Remove a given target by calling its 'exists()' and 'remove()' methods."""
    try:
        if target.exists():
            try:
                L.debug("Removing %s", target.path)
            except AttributeError:  # pragma: no cover
                pass
            target.remove()
    except AttributeError as e:
        raise AttributeError("The target must have 'exists()' and 'remove()' methods") from e


def apply_over_luigi_iterable(luigi_iterable, func):
    """Apply the given function to a luigi iterable (task.input() or task.output())."""
    try:
        results = {}
        for key, i in luigi_iterable.items():
            results[key] = func(i, key)
    except AttributeError:
        results = []
        for i in luigi.task.flatten(luigi_iterable):
            results.append(func(i))
    return results


def apply_over_inputs(task, func):
    """Apply the given function to all inputs of a luigi task.

    The given function should accept the following arguments:
    * task_output: the output(s) of the required task(s) of the given task
    * key=None: the key when the iterable is a dictionnary
    """
    inputs = task.input()
    return apply_over_luigi_iterable(inputs, func)


def apply_over_outputs(task, func):
    """Apply the given function to all outputs of a luigi task.

    The given function should accept the following arguments:
    * target: the output target(s) of the given task
    * key=None: the key when the iterable is a dictionnary
    """
    outputs = task.output()

    return apply_over_luigi_iterable(outputs, func)


def get_dependency_graph(task):
    """Compute dependency graph of a given task.

    Args:
        task (luigi.Task): the task from which the dependency graph is computed.

    Returns:
        list(luigi.task): A list of (parent, child) tuples
    """
    childs = []
    for t in task.deps():
        childs.append((task, t))
        childs.extend(get_dependency_graph(t))
    return childs


def _param_repr(description, default):
    """Produce parameter help string representation for sphinx-doc."""
    if description:
        help_str = description
    else:
        help_str = ""
    if default is not PARAM_NO_VALUE:
        help_str += f"({default})"
    return help_str
