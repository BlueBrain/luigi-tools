"""Tests for luigi tools."""
from pathlib import Path

import luigi
import pytest
from luigi.parameter import _no_value as PARAM_NO_VALUE

import luigi_tools.task
import luigi_tools.target
import luigi_tools.util

from .tools import create_not_empty_file


DATA = Path(__file__).parent / "data"


def test_target_remove(tmpdir):
    class TaskA(luigi_tools.task.WorkflowTask):
        """"""

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
    class TaskA(luigi.Task):
        a = luigi.Parameter(default="a")

        def output(self):
            return self.a

    class TaskB(luigi.Task):
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
    class TaskA(luigi.Task):
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

    # Test graphviz_dependency_graph()
    dot = luigi_tools.util.graphviz_dependency_graph(graph)
    assert dot.body == [
        "\tTaskE [color=red penwidth=1.5]",
        "\tTaskD",
        "\tTaskE -> TaskD",
        "\tTaskB",
        "\tTaskD -> TaskB",
        "\tTaskA",
        "\tTaskB -> TaskA",
        "\tTaskC",
        "\tTaskD -> TaskC",
        "\tTaskA",
        "\tTaskC -> TaskA",
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
    from graphviz import Digraph

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
        "\tTaskE [color=blue penwidth=1.5]",
        "\tcustom_name",
        "\tTaskE -> custom_name",
        "\tTaskB",
        "\tcustom_name -> TaskB",
        '\tTaskA [custom_attr="custom value"]',
        '\tTaskB -> TaskA [label="custom label"]',
        "\tTaskC",
        "\tcustom_name -> TaskC",
        '\tTaskA [custom_attr="custom value"]',
        "\tTaskC -> TaskA",
    ]
    assert dot_with_attrs.node_attr == {
        "shape": "box",
        "fontsize": "10",
        "height": "0.5",
        "fontname": '"Vera Sans, DejaVu Sans, Liberation Sans, Arial, Helvetica, sans"',
        "style": "setlinewidth(0.5),filled",
        "fillcolor": "white",
    }
    assert dot_with_attrs.edge_attr == {"arrowsize": "0.75", "style": "setlinewidth(0.5)"}

    # Test graphviz_dependency_graph() with empty graph
    with pytest.raises(ValueError):
        luigi_tools.util.graphviz_dependency_graph([])

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


def test_param_repr():
    assert luigi_tools.util._param_repr(None, PARAM_NO_VALUE) == ""
    assert luigi_tools.util._param_repr(None, None) == "(None)"
    assert luigi_tools.util._param_repr("description", None) == "description(None)"
    assert luigi_tools.util._param_repr("description", "default") == "description(default)"
