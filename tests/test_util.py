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

"""Tests for luigi-tools utils."""

# pylint: disable=empty-docstring
# pylint: disable=protected-access
# pylint: disable=unused-argument
import copy
from configparser import ConfigParser
from pathlib import Path

import luigi
import pytest
from graphviz import Digraph
from luigi.parameter import _no_value as PARAM_NO_VALUE

import luigi_tools
import luigi_tools.target
import luigi_tools.task
import luigi_tools.util
from luigi_tools.util import set_luigi_config

from .tools import create_not_empty_file

DATA = Path(__file__).parent / "data"


def test_target_remove(tmpdir):
    """Test the `luigi_tools.util.target_remove` function."""

    class TaskA(luigi_tools.task.WorkflowTask):
        """A simple test task."""

        def run(self):
            for i in luigi.task.flatten(self.output()):
                i.makedirs()

            Path(self.output()[0].path).mkdir()
            create_not_empty_file(self.output()[0].path + "/file.test")
            create_not_empty_file(self.output()[1].path)

            for i in luigi.task.flatten(self.output()):
                assert i.exists()
                luigi_tools.util.target_remove(i)
                assert not i.exists()

        def output(self):
            return [
                luigi.LocalTarget(tmpdir / "TaskA"),
                luigi.LocalTarget(tmpdir / "TaskA_bis" / "file.test"),
            ]

    assert luigi.build([TaskA()], local_scheduler=True)


def test_apply_over_inputs():
    """Test the `luigi_tools.util.apply_over_inputs` function."""

    class TaskA(luigi.Task):
        """A simple test task."""

        a = luigi.Parameter(default="a")

        def output(self):
            return self.a

    class TaskB(luigi.Task):
        """A simple test task."""

        def requires(self):
            return {
                "a": TaskA(),
                "b": TaskA(a="b"),
                "c": [TaskA(a="c1"), TaskA(a="c2")],
            }

    def get_a(task_output, key=None):
        return key, task_output

    task = TaskB()
    res = luigi_tools.util.apply_over_inputs(task, get_a)
    assert res == {"a": ("a", "a"), "b": ("b", "b"), "c": ("c", ["c1", "c2"])}


def test_apply_over_outputs():
    """Test the `luigi_tools.util.apply_over_outputs` function."""

    class TaskA(luigi.Task):
        """A simple test task."""

        a = luigi.Parameter(default="a")

        def output(self):
            return {
                "a": self.a,
                "b": self.a + "_b",
                "c": [self.a + "_c1", self.a + "_c2"],
            }

    def get_a(task_output, key=None):
        return key, task_output

    task = TaskA(a="test")
    res = luigi_tools.util.apply_over_outputs(task, get_a)
    assert res == {
        "a": ("a", "test"),
        "b": ("b", "test_b"),
        "c": ("c", ["test_c1", "test_c2"]),
    }


def test_dependency_graph(tmpdir, task_collection):
    """Test dependency graph."""
    # pylint: disable=use-implicit-booleaness-not-comparison
    start = task_collection.TaskE()

    # Test get_dependency_graph()
    graph = luigi_tools.util.get_dependency_graph(start)
    assert graph == [
        (task_collection.TaskE(), task_collection.TaskD()),
        (task_collection.TaskD(), task_collection.TaskB()),
        (task_collection.TaskB(), task_collection.TaskA()),
        (task_collection.TaskD(), task_collection.TaskC()),
        (task_collection.TaskC(), task_collection.TaskA()),
    ]

    # Test get_dependency_graph() with only one node
    assert luigi_tools.util.get_dependency_graph(task_collection.TaskA()) == []
    assert luigi_tools.util.get_dependency_graph(task_collection.TaskA(), allow_orphans=True) == [
        (task_collection.TaskA(), None)
    ]

    # Test graphviz_dependency_graph()
    dot = luigi_tools.util.graphviz_dependency_graph(graph)
    assert dot.body == [
        "\tTaskE [color=red penwidth=1.5]\n",
        "\tTaskD\n",
        "\tTaskE -> TaskD\n",
        "\tTaskB\n",
        "\tTaskD -> TaskB\n",
        "\tTaskA\n",
        "\tTaskB -> TaskA\n",
        "\tTaskC\n",
        "\tTaskD -> TaskC\n",
        "\tTaskA\n",
        "\tTaskC -> TaskA\n",
    ]
    assert dot.node_attr == {
        "shape": "box",
        "fontsize": "9",
        "height": "0.25",
        "fontname": '"Vera Sans, DejaVu Sans, Liberation Sans, Arial, Helvetica, sans"',
        "style": "setlinewidth(0.5),filled",
        "fillcolor": "white",
    }
    assert dot.edge_attr == {"arrowsize": "0.5", "style": "setlinewidth(0.5)"}

    # Test graphviz_dependency_graph() with custom attributes
    dot_with_attrs = luigi_tools.util.graphviz_dependency_graph(
        graph,
        graph_attrs={"bgcolor": "red"},
        node_attrs={"fontsize": "10", "height": "0.5"},
        edge_attrs={"arrowsize": "0.75"},
        root_attrs={"color": "blue"},
        task_names={task_collection.TaskD(): "custom_name"},
        graphviz_class=Digraph,
        node_kwargs={task_collection.TaskA(): {"custom_attr": "custom value"}},
        edge_kwargs={(task_collection.TaskB(), task_collection.TaskA()): {"label": "custom label"}},
    )
    assert dot_with_attrs.body == [
        "\tTaskE [color=blue penwidth=1.5]\n",
        "\tcustom_name\n",
        "\tTaskE -> custom_name\n",
        "\tTaskB\n",
        "\tcustom_name -> TaskB\n",
        '\tTaskA [custom_attr="custom value"]\n',
        '\tTaskB -> TaskA [label="custom label"]\n',
        "\tTaskC\n",
        "\tcustom_name -> TaskC\n",
        '\tTaskA [custom_attr="custom value"]\n',
        "\tTaskC -> TaskA\n",
    ]
    assert dot_with_attrs.node_attr == {
        "shape": "box",
        "fontsize": "10",
        "height": "0.5",
        "fontname": '"Vera Sans, DejaVu Sans, Liberation Sans, Arial, Helvetica, sans"',
        "style": "setlinewidth(0.5),filled",
        "fillcolor": "white",
    }
    assert dot_with_attrs.edge_attr == {
        "arrowsize": "0.75",
        "style": "setlinewidth(0.5)",
    }

    # Test graphviz_dependency_graph() with empty graph
    with pytest.raises(ValueError):
        luigi_tools.util.graphviz_dependency_graph([])

    # Test graphviz_dependency_graph() with only one node
    dot_one_node_orphan = luigi_tools.util.graphviz_dependency_graph(
        [(task_collection.TaskA(), None)]
    )
    assert dot_one_node_orphan.source.count("node ") == 1

    # Test render_dependency_graph()
    output_file = Path(tmpdir / "test_dependency_graph.png")
    luigi_tools.util.render_dependency_graph(
        dot,
        str(output_file),
    )

    assert output_file.exists()

    # Test render_dependency_graph() with given format
    output_file = Path(tmpdir / "test_dependency_graph_format.pdf")
    luigi_tools.util.render_dependency_graph(
        dot,
        str(output_file),
        format="png",
    )

    assert not output_file.exists()
    assert output_file.with_suffix(".png").exists()

    # Test export_dependency_graph()
    output_file = Path(tmpdir / "test_export_dependency_graph.pdf")
    luigi_tools.util.export_dependency_graph(
        start,
        str(output_file),
        allow_orphans=True,
        graph_attrs={"bgcolor": "red"},
        node_attrs={"fontsize": "10", "height": "0.5"},
        edge_attrs={"arrowsize": "0.75"},
        root_attrs={"color": "blue"},
        task_names={task_collection.TaskD(): "custom_name"},
        node_kwargs={task_collection.TaskA(): {"custom_attr": "custom value"}},
        edge_kwargs={(task_collection.TaskB(), task_collection.TaskA()): {"label": "custom label"}},
        graphviz_class=Digraph,
        format="png",
    )

    assert not output_file.exists()
    assert output_file.with_suffix(".png").exists()


def test_param_repr():
    """Test the `luigi_tools.util._param_repr` function."""
    assert luigi_tools.util._param_repr(None, PARAM_NO_VALUE) == ""
    assert luigi_tools.util._param_repr(None, None) == "(None)"
    assert luigi_tools.util._param_repr("description", None) == "description(None)"
    assert luigi_tools.util._param_repr("description", "default") == "description(default)"


class TestRegisterTemplates:
    """Test the configuration template mechanism."""

    @pytest.fixture
    def config_reseter(self):
        """Fixture to reset the luigi configuration."""
        cfg_cls = luigi.configuration.cfg_parser.LuigiConfigParser
        current_config = copy.deepcopy(cfg_cls._config_paths)
        yield
        cfg_cls._config_paths = current_config
        cfg_cls.reload()
        luigi.configuration.get_config().clear()

    @pytest.fixture
    def Task(self, tmpdir):
        """A simple test task."""

        class Task(luigi.Task):
            """A simple test task."""

            a = luigi.Parameter(default="a")
            expected_a = luigi.Parameter(default="a")

            def run(self):
                assert self.a == self.expected_a

            def output(self):
                return tmpdir / "not_existing_file"

        return Task

    @pytest.fixture
    def template_dir(self, tmpdir):
        """Create a dummy template directory."""
        template_dir = tmpdir / "templates"
        template_dir.mkdir()
        config = ConfigParser()
        config.read_dict({"Task": {"a": "a_from_template"}})
        with open(template_dir / "template_1.cfg", "w", encoding="utf-8") as f:
            config.write(f)
        return str(template_dir)

    def test_cfg_only(self, Task, template_dir, config_reseter):
        """Test with only a config file."""
        with set_luigi_config(
            {
                "Task": {"a": "a_from_cfg"},
            }
        ):
            luigi_tools.util.register_templates(template_dir, "template_1")
            assert luigi.build([Task(expected_a="a_from_cfg")], local_scheduler=True)

    def test_template_only(self, Task, template_dir, config_reseter):
        """Test with only a template."""
        with set_luigi_config(
            {
                "Template": {"name": "template_1"},
            }
        ):
            luigi_tools.util.register_templates(template_dir)
            assert luigi.build([Task(expected_a="a_from_template")], local_scheduler=True)

    def test_template_and_cfg(self, Task, template_dir, config_reseter):
        """Test with both a config file and a template."""
        with set_luigi_config(
            {
                "Template": {"name": "template_1"},
                "Task": {"a": "a_from_cfg"},
            }
        ):
            luigi_tools.util.register_templates(template_dir)
            assert luigi.build([Task(expected_a="a_from_cfg")], local_scheduler=True)

    def test_template_and_cfg_and_env_var(self, Task, template_dir, config_reseter, monkeypatch):
        """Test with both a config file and a template."""
        extra_cfg_file = str(Path(template_dir) / "extra.cfg")
        config = ConfigParser()
        config.read_dict({"Task": {"a": "a_from_extra_cfg_file"}})
        with open(extra_cfg_file, "w", encoding="utf-8") as f:
            config.write(f)

        monkeypatch.setenv("LUIGI_CONFIG_PATH", extra_cfg_file, prepend=False)

        with set_luigi_config(
            {
                "Template": {"name": "template_1"},
                "Task": {"a": "a_from_cfg"},
            }
        ):
            luigi_tools.util.register_templates(template_dir)
            assert luigi.build([Task(expected_a="a_from_extra_cfg_file")], local_scheduler=True)

    def test_missing_template(self, Task, template_dir, config_reseter):
        """Test with a missing template."""
        with set_luigi_config(
            {
                "Template": {"name": "missing_template"},
                "Task": {"a": "a_from_cfg"},
            }
        ):
            with pytest.raises(ValueError, match=r"The template .* could not be found\."):
                luigi_tools.util.register_templates(template_dir)

    def test_template_directory_in_cfg(self, Task, template_dir, config_reseter):
        """Test with a template directory defined in the luigi config file."""
        with set_luigi_config(
            {
                "Template": {
                    "name": "template_1",
                    "directory": str(template_dir),
                },
                "Task": {"a": "a_from_cfg"},
            }
        ):
            luigi_tools.util.register_templates()
            assert luigi.build([Task(expected_a="a_from_cfg")], local_scheduler=True)

    def test_template_directory_override(self, Task, template_dir, config_reseter):
        """Test with a template directory defined in luigi.cfg but overridden in the task ctor."""
        directory = Path(template_dir)
        new_directory = directory.with_name("new_templates")
        directory.rename(new_directory)
        with set_luigi_config(
            {
                "Template": {
                    "name": "template_1",
                    "directory": str(new_directory),
                }
            }
        ):
            luigi_tools.util.register_templates(template_dir)
            assert luigi.build([Task(expected_a="a_from_template")], local_scheduler=True)

    def test_no_directory(self, Task, config_reseter):
        """Test without template directory given to the template registering function."""
        with set_luigi_config(
            {
                "Template": {"name": "template_name"},
            }
        ):
            msg = (
                r"A directory must either be given to this function or in the \[Template\] section "
                r"of the luigi\.cfg file\."
            )
            with pytest.raises(ValueError, match=msg):
                luigi_tools.util.register_templates()

    def test_no_name(self, Task, config_reseter):
        """Test with no template name given."""
        with set_luigi_config(
            {
                "Template": {"directory": "any directory"},
            }
        ):
            msg = (
                r"A name must either be given to this function or in the \[Template\] section of "
                r"the luigi\.cfg file\."
            )
            with pytest.raises(ValueError, match=msg):
                luigi_tools.util.register_templates()

    def test_no_hierarchy_end(self, Task, template_dir, config_reseter):
        """Test the hierarchy_end parameter."""
        # Register the template only
        luigi_tools.util.register_templates(template_dir, "template_1", hierarchy_end=False)

        # Rename and update the template
        directory = Path(template_dir)
        new_directory = directory.with_name("new_templates")
        new_directory.mkdir()
        config = ConfigParser()
        config.read_dict({"Task": {"a": "a_from_2nd_template"}})
        with open(new_directory / "template_2.cfg", "w", encoding="utf-8") as f:
            config.write(f)

        # Register the new template and the luigi.cfg file
        luigi_tools.util.register_templates(new_directory, "template_2")
        assert luigi.build([Task(expected_a="a_from_2nd_template")], local_scheduler=True)


class TestSetLuigiConfig:
    """Test the `luigi_tools.util.set_luigi_config` function."""

    @pytest.fixture
    def Task(self, tmpdir):
        """A simple test task."""

        class Task(luigi.Task):
            """A simple test task."""

            a = luigi.Parameter(default="a")
            expected_a = luigi.Parameter(default="a")

            def run(self):
                assert self.a == self.expected_a

            def output(self):
                return tmpdir / "not_existing_file"

        return Task

    def test_defaults(self, Task):
        """Test with empty config."""
        assert luigi.build([Task()], local_scheduler=True)

        with set_luigi_config():
            assert luigi.build([Task()], local_scheduler=True)

    def test_new_config(self, Task):
        """Test with new config."""
        with set_luigi_config(
            {
                "Task": {"a": "a_from_cfg"},
            }
        ):
            assert luigi.build([Task(expected_a="a_from_cfg")], local_scheduler=True)

        failed_task = []
        exceptions = []

        @Task.event_handler(luigi.Event.FAILURE)
        def check_exception(task, exception):
            failed_task.append(str(task))
            exceptions.append(str(exception))

        with set_luigi_config(
            {
                "Task": {"a": "a_from_cfg"},
            }
        ):
            assert not luigi.build([Task(expected_a="different_value")], local_scheduler=True)

        assert failed_task == [str(Task(a="a_from_cfg", expected_a="different_value"))]
        assert exceptions == [
            "assert 'a_from_cfg' == 'different_value'\n  \n  - different_value\n  + a_from_cfg"
        ]

    def test_config_file(self, Task):
        """Test with a new config and a config file."""
        filename = "test_config_file.cfg"
        with set_luigi_config(
            {
                "Task": {"a": "a_from_cfg"},
            },
            filename,
        ):
            assert luigi.build([Task(expected_a="a_from_cfg")], local_scheduler=True)
            assert Path(filename).exists()
        assert not Path(filename).exists()

    def test_params_ConfigParser(self, Task):
        """Test using an `ConfigParser` instead of a `dict`.."""
        params = {
            "Task": {"a": "a_from_cfg"},
        }
        config = ConfigParser()
        config.read_dict(params)
        with set_luigi_config(config):
            assert luigi.build([Task(expected_a="a_from_cfg")], local_scheduler=True)


def test_deprecation_warning():
    """Test the deprecation warnings."""
    with pytest.warns(
        luigi_tools.MovedToLuigiWarning,
        match="This feature was moved to the luigi package and is available since version 1.2.3.",
    ):
        luigi_tools.moved_to_luigi_warning("1.2.3")

    with pytest.warns(
        luigi_tools.MovedToLuigiWarning,
        match=(
            "This feature was moved to the luigi package and is available since version 1.2.3. It "
            "will be deprecated in version 3.4.5."
        ),
    ):
        luigi_tools.moved_to_luigi_warning("1.2.3", deprecation_version="3.4.5")

    with pytest.warns(
        luigi_tools.MovedToLuigiWarning,
        match=(
            "This feature was moved to the luigi package and will be available after version 1.2.3."
        ),
    ):
        luigi_tools.moved_to_luigi_warning(previous_luigi_version="1.2.3")

    with pytest.warns(
        luigi_tools.MovedToLuigiWarning,
        match=(
            "This feature was moved to the luigi package and will be available after version 1.2.3."
            " It will be deprecated in version 3.4.5."
        ),
    ):
        luigi_tools.moved_to_luigi_warning(
            previous_luigi_version="1.2.3", deprecation_version="3.4.5"
        )

    with pytest.raises(
        ValueError,
        match=(
            "Either the 'luigi_version' or the 'previous_luigi_version' argument must be not None "
            "but not both of them"
        ),
    ):
        luigi_tools.moved_to_luigi_warning()

    with pytest.raises(
        ValueError,
        match=(
            "Either the 'luigi_version' or the 'previous_luigi_version' argument must be not None "
            "but not both of them"
        ),
    ):
        luigi_tools.moved_to_luigi_warning("1.2.3", "3.4.5")


def test_luigi_config_to_dict():
    """Test that the transformation of a luigi config file is properly loaded into a dict."""
    input_cfg = {
        "Task": {"a": "a_from_cfg"},
    }
    with set_luigi_config(input_cfg):
        cfg = luigi_tools.util.luigi_config_to_dict()
    assert cfg == input_cfg

    filename = "another_luigi.cfg"
    with set_luigi_config(input_cfg, filename):
        cfg = luigi_tools.util.luigi_config_to_dict(filename)
    assert cfg == input_cfg
