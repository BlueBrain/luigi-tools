"""This module provides some fonctions to work with luigi tasks."""
import logging
import re
from pathlib import Path

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
    """Apply the given function to all inputs of a :class:`luigi.Task`.

    The given function should accept the following arguments:
        * task_output: the output(s) of the required task(s) of the given task
        * key=None: the key when the iterable is a dictionnary
    """
    inputs = task.input()
    return apply_over_luigi_iterable(inputs, func)


def apply_over_outputs(task, func):
    """Apply the given function to all outputs of a :class:`luigi.Task`.

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
        list(luigi.Task): A list of (parent, child) tuples.
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


def graphviz_dependency_graph(
    g,
    graph_attrs=None,
    node_attrs=None,
    edge_attrs=None,
    root_attrs=None,
    task_names=None,
    node_kwargs=None,
    edge_kwargs=None,
    graphviz_class=None,
):
    """Create a GraphViz dependency graph.

    Args:
        g (list(tuple(luigi.Task))): A list of tuples of :class:`luigi.Task` objects, usually
            created with :mod:`luigi_tools.util.get_dependency_graph()`.
        graph_attrs (dict): The graph attributes that will override the defaults.
        node_attrs (dict): The node attributes that will override the defaults.
        edge_attrs (dict): The edge attributes that will override the defaults.
        root_attrs (dict): The specific node attributes only used for the root that will override
            the defaults.
        task_names (dict): A dictionary with :class:`luigi.Task` objects as keys and custom names as
            values.
        node_kwargs (dict): A dictionary with :class:`luigi.Task` objects as keys and the kwargs
            given to :meth:`graphviz.Digraph.node()` as values.
        edge_kwargs (dict): A dictionary with :class:`luigi.Task` objects as keys and the kwargs
            given to :meth:`graphviz.Digraph.edge()` as values.
        graphviz_class (graphviz.Graph or graphviz.Digraph): The class used to store the graph.

    Returns:
        The Graph or Digraph created.
    """
    if len(g) <= 0:
        raise ValueError("The dependency graph is empty")

    # Setup default values if they are not given
    if graphviz_class is None:
        try:
            from graphviz import Digraph  # pylint: disable=import-outside-toplevel

            graphviz_class = Digraph
        except ImportError as e:  # pragma: no cover
            msg = "Could not import GraphViz, please install it."
            raise ImportError(msg) from e

    default_graph_attrs = {
        "size": "7.0, 15.0",
        "bgcolor": "transparent",
        "fontsize": "9",
        "layout": "dot",
        "rankdir": "TB",
    }
    if graph_attrs is not None:
        default_graph_attrs.update(graph_attrs)

    default_node_attrs = {
        "shape": "box",
        "fontsize": "9",
        "height": "0.25",
        "fontname": '"Vera Sans, DejaVu Sans, Liberation Sans, Arial, Helvetica, sans"',
        "style": "setlinewidth(0.5),filled",
        "fillcolor": "white",
    }
    if node_attrs is not None:
        default_node_attrs.update(node_attrs)

    default_edge_attrs = {
        "arrowsize": "0.5",
        "style": "setlinewidth(0.5)",
    }
    if edge_attrs is not None:
        default_edge_attrs.update(edge_attrs)

    default_root_attrs = {"color": "red", "penwidth": "1.5"}
    if root_attrs is not None:
        default_root_attrs.update(root_attrs)

    if not task_names:
        task_names = {}

    if not node_kwargs:
        node_kwargs = {}

    if not edge_kwargs:
        edge_kwargs = {}

    # Build the dependency graph
    dot = graphviz_class(
        comment="Dependency graph",
        strict=True,
        graph_attr=default_graph_attrs,
        node_attr=default_node_attrs,
        edge_attr=default_edge_attrs,
    )

    root = g[0][0]
    task_name = task_names.get(root, root.__class__.__name__)
    dot.node(task_name, **default_root_attrs)

    for parent, child in g:
        parent_name = task_names.get(parent, parent.__class__.__name__)
        child_name = task_names.get(child, child.__class__.__name__)
        dot.node(child_name, **node_kwargs.get(child, {}))
        dot.edge(parent_name, child_name, **edge_kwargs.get((parent, child), {}))

    return dot


def render_dependency_graph(graph, filepath, **kwargs):
    """Render a dependency graph using GraphViz.

    Args:
        graph (graphviz.Graph or graphviz.Digraph): The graph to render, usually create with
            :mod:`luigi_tools.util.graphviz_dependency_graph()`.
        filepath (str): The path to the rendered file. If the ``format`` kwarg is not given, this
            path should contain an extension. Otherwise, the extension is guessed from the format.
        kwargs: The kwargs are passed to the :meth:`graphviz.Digraph.render()` function.
    """
    filepath = Path(filepath)
    filename = filepath.with_suffix("")
    fileformat = kwargs.pop("format", None)
    if not fileformat:
        pattern = re.compile(r"\.?(.*)")
        match = re.match(pattern, filepath.suffix)
        fileformat = match.group(1) or None
    cleanup = kwargs.pop("cleanup", True)
    graph.render(filename=filename, format=fileformat, cleanup=cleanup, **kwargs)
