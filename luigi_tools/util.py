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

"""This module provides some functions to work with luigi tasks."""
import configparser
import logging
import os
import re
import warnings
from configparser import ConfigParser
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
        * key=None: the key when the iterable is a dictionary
    """
    inputs = task.input()
    return apply_over_luigi_iterable(inputs, func)


def apply_over_outputs(task, func):
    """Apply the given function to all outputs of a :class:`luigi.Task`.

    The given function should accept the following arguments:
        * target: the output target(s) of the given task
        * key=None: the key when the iterable is a dictionary
    """
    outputs = task.output()

    return apply_over_luigi_iterable(outputs, func)


def apply_over_required(task, func):
    """Apply the given function to all required tasks of a :class:`luigi.WrapperTask`.

    The given function should accept the following arguments:
        * target: the output target(s) of the given task
        * key=None: the key when the iterable is a dictionary
    """
    required = task.requires()

    return apply_over_luigi_iterable(required, func)


def get_dependency_graph(
    task,
    allow_orphans=False,
):
    """Compute dependency graph of a given task.

    Args:
        task (luigi.Task): the task from which the dependency graph is computed.
        allow_orphans (bool): If set to True, orphan nodes are returned with a None child.

    Returns:
        list(luigi.Task): A list of (parent, child) tuples.
    """
    children = []
    for t in task.deps():
        children.append((task, t))
        children.extend(get_dependency_graph(t))
    if not children and allow_orphans:
        children.append((task, None))
    return children


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
        "dpi": "300",
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
        if child is None:
            continue
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


def export_dependency_graph(
    task,
    filepath="dependency_graph.png",
    allow_orphans=False,
    graph_attrs=None,
    node_attrs=None,
    edge_attrs=None,
    root_attrs=None,
    task_names=None,
    node_kwargs=None,
    edge_kwargs=None,
    graphviz_class=None,
    **render_kwargs,
):
    """Build and export a dependency graph.

    Args:
        task (luigi.Task): the task from which the dependency graph is computed.
        filepath (str): The path to the rendered file. If the ``format`` kwarg is not given, this
            path should contain an extension. Otherwise, the extension is guessed from the format.
        allow_orphans (bool): If set to True, orphan nodes are returned with a None child.
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

    Keyword Args:
        render_kwargs: The keyword arguments are passed to the :meth:`graphviz.Digraph.render()`
            function.
    """
    # pylint: disable=too-many-arguments
    deps = get_dependency_graph(task, allow_orphans=allow_orphans)
    graph = graphviz_dependency_graph(
        deps,
        graph_attrs=graph_attrs,
        node_attrs=node_attrs,
        edge_attrs=edge_attrs,
        root_attrs=root_attrs,
        task_names=task_names,
        node_kwargs=node_kwargs,
        edge_kwargs=edge_kwargs,
        graphviz_class=graphviz_class,
    )
    render_dependency_graph(graph, filepath, **render_kwargs)


def register_templates(directory=None, name=None, hierarchy_end=True):
    """Add INI templates to the config file list processed by luigi.

    This function should be used before the :class:`luigi.Task` are processed, so it should usually
    be used in the ``__init__.py`` of your package.

    In order to use a template, a ``Template`` section entry must be added to the luigi.cfg of the
    project. This section should contain at least one entry with the name of the template to use,
    and, optionally, another entry with the path to the directory containing the templates, as the
    following example:

    .. code-block:: INI

        [Template]
        name = template_name
        directory = /path/to/the/template/directory

    If the ``directory`` or the ``name`` entries are given in the ``luigi.cfg`` file, they override
    the arguments given to this function.

    A template file should be similar to a ``luigi.cfg`` file with only the entries for which
    specific default values should be defined. The file should named according to the template
    name. For example, the template named ``template_1`` should be a INI file located in the given
    directory and named ``template_1.cfg``.

    Args:
        directory (str): Path to the directory containing the template files.
        name (str): The name of the template to use.
        hierarchy_end (bool): If set to False, the ``luigi.cfg`` entry is not added after the
            template entry, so it is possible to combine several templates before processing
            the ``luigi.cfg`` file.
    """
    base_config = luigi.configuration.get_config()

    if "Template" in base_config:
        template_config = base_config["Template"]
    else:
        template_config = {}

    template_path = template_config.get("directory", directory)
    if template_path is None:
        raise ValueError(
            "A directory must either be given to this function or in the [Template] section of "
            "the luigi.cfg file."
        )
    template_path = Path(template_path)

    template_name = template_config.get("name", name)
    if template_name is None:
        raise ValueError(
            "A name must either be given to this function or in the [Template] section of "
            "the luigi.cfg file."
        )

    template = (template_path / template_name).with_suffix(".cfg")
    if not template.exists():
        raise ValueError(f"The template '{template}' could not be found.")

    luigi.configuration.add_config_path(template)
    if hierarchy_end:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Config file does not exist.*")
            luigi.configuration.add_config_path("luigi.cfg")
            env_cfg = os.environ.get("LUIGI_CONFIG_PATH")
            if env_cfg is not None:
                luigi.configuration.add_config_path(env_cfg)


class set_luigi_config:
    """Context manager to set current luigi config.

    Args:
        params (dict): The parameters to load into the luigi configuration.
        configfile (str): Path to the temporary config file (luigi.cfg by default).
    """

    def __init__(self, params=None, configfile=None):
        self.params = params
        self.luigi_config = luigi.configuration.get_config()
        if configfile is not None:
            self.configfile = configfile
        else:
            self.configfile = "luigi.cfg"

    def __enter__(self):
        """Load the given luigi configuration."""
        # Reset luigi config
        self.luigi_config.clear()

        # Remove config file
        if os.path.exists(self.configfile):
            os.remove(self.configfile)

        # Export config
        if self.params is not None:
            self.export_config(self.configfile)

            # Set current config in luigi
            self.luigi_config.read(self.configfile)
        else:
            self.configfile = None

    def get_config(self):
        """Convert the parameter dict to a :class:`configparser.ConfigParser` object."""
        config = ConfigParser()
        config.read_dict(self.params)
        return config

    def export_config(self, filepath):
        """Export the configuration to the configuration file."""
        params = self.get_config()

        # Export params
        with open(filepath, "w") as configfile:  # pylint: disable=unspecified-encoding
            params.write(configfile)

    def __exit__(self, *args):
        """Reset the configuration when exiting the context manager."""
        # Remove config file
        if self.configfile is not None and os.path.exists(self.configfile):
            os.remove(self.configfile)

        # Reset luigi config
        self.luigi_config.clear()


def configparser_to_dict(luigi_config):
    """Transform a ConfigParser object to a dict."""
    dict_config = {}
    for section in luigi_config.sections():
        dict_config[section] = {}
        for option in luigi_config.options(section):
            dict_config[section][option] = luigi_config.get(section, option)
    return dict_config


def luigi_config_to_dict(filename="luigi.cfg"):
    """Load a luigi config file to a dict."""
    luigi_config = configparser.ConfigParser()
    luigi_config.read(filename)
    return configparser_to_dict(luigi_config)
