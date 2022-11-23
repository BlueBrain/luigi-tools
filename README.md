[![Build status](https://github.com/BlueBrain/luigi-tools/actions/workflows/run-tox.yml/badge.svg?branch=main)](https://github.com/BlueBrain/luigi-tools/actions)
[![License](https://img.shields.io/badge/License-Apache%202-blue)](https://github.com/BlueBrain/luigi-tools/blob/master/LICENSE.txt)
[![Documentation status](https://readthedocs.org/projects/luigi-tools/badge/?version=latest)](https://luigi-tools.readthedocs.io/)


# Luigi-tools

This package extends and adds new features to the [luigi package][luigi_url].
Here are a few examples of these features:

* add a new `BoolParameter` that automatically switch to explicit parsing when the default value is `True` (otherwise it is not possible to set it to `False` using the CLI).
* add several types of optional parameters.
* add a `OutputLocalTarget` class to help building an output tree.
* add a mixin that adds a `--rerun` parameter that forces a given task to run again even if its targets exist, and also rerun all the tasks that depend on this one.
* add a mixin to remove the output of failed tasks which is likely to be corrupted or incomplete.
This feature applies the default behaviour of a [snakemake][snakemake_url] rule (Task).
* add a new `@copy_params` mechanism to copy the parameters from a task to another (the `@inherits` gives the same object to all the inheriting tasks while `@copy_params` only copies the definition of the parameter so each inheriting task can be given a different value).
* add functions to get and display the dependency graph of a given task.
* add a mechanism to setup templates for the `luigi.cfg` files, so the user just has to update specific values instead of copying the entire `luigi.cfg`.

Please read the [complete API documentation][luigi_tools_api_url] for more details.

## Installation

This package should be installed using pip:

```bash
pip install luigi-tools
```

## Usage

The [Luigi][luigi_url] package describes itself as follow:

> Luigi is a Python package that helps you build complex pipelines of batch
> jobs. It handles dependency resolution, workflow management, visualization, handling failures,
> command line integration, and much more.

The [luigi-tools][luigi_tools_url] package is supposed to make luigi easier for developers.
The following presents a few examples of the main features of the package.

### Boolean parameter

The [luigi.BoolParameter](https://luigi.readthedocs.io/en/stable/api/luigi.parameter.html#luigi.parameter.BoolParameter)
can be parsed in two ways: implicit or explicit. The explicit way requires the user to enter a
value: `True` of `False`. On the contrary, the implicit way requires no value and will just set
the value to `True` if the parameter is given. This is not compatible with a default value set to
`True`, as it is not possible to set the value to `False` using the CLI in this case.

If you want to automatically set the parsing to explicit when the default value is `True`:

```python
from luigi.task import Task
from luigi_tools import BoolParameter

class MyTask(Task):

    a_boolean_parameter = BoolParameter(default=True)

    def run(self):
        pass
```

### Target with prefix

The [Luigi][luigi_url] workflows are based on `Target` object that represents the state of a step
of the workflow. These targets can be anything but are often files in a result directory tree. In
order to not having to specify the result directory to each target, one can use the
`OutputLocalTarget` class and give it a `prefix`. So all targets based on this class will be
located in the same directory.

```python
from luigi.task import Task
from luigi_tools.target import OutputLocalTarget

class MyTask(Task):

    def run(self):
        pass

    def output(self):
        # The target will point to the file result_directory/filename.ext
        return OutputLocalTarget("filename.ext")

# Set the default prefix (it could also be called inside another Task)
OutputLocalTarget.set_default_prefix("result_directory")

# Run the task (the task can also be called with the CLI as usual)
luigi.build([MyTask()], local_scheduler=True)
```

### Rerunable task

In [Luigi][luigi_url], the states of the tasks are deducted from their targets. If the targets exist, the task
is assumed to have already been completed and is thus skipped if the workflow is run again. This
behavior is usually good to avoid performing computations that are already completed. Nevertheless,
sometimes it is desirable to overwrite a former result, especially during the development process.
For this reason, a mixin that adds a `--rerun` parameter to a task is introduced. When this
parameter is set to `True`, all the targets of this task are deleted as well as the targets of the
tasks that depend on this one. So when all the tasks that are related to this task will run again.
As for any mixin, it must be go on the left of the `Task` class in the inheritance list.

```python
from luigi.task import Task
from luigi_tools.task import RerunMixin

class MyTask(RerunMixin, Task):

    def run(self):
        pass
```

Now the task `MyTask` has a boolean parameter `--rerun` which can be called in the CLI:

```bash
luigi -m my_module mytask --rerun
luigi -m my_module another_task_that_depends_on_mytask --MyTask-rerun
```

### Clear the output of failed tasks

When a task fails unexpectedly, it may leave an incomplete or corrupted output
that leads to wrong results in the downstream. With the RemoveCorruptedOutputMixin,
Luigi automatically removes the output targets of the tasks that failed. This is the default behaviour
in other workflow management systems such as [Snakemake][snakemake_url].

```python
from luigi_tools.task import RemoveCorruptedOutputMixin

    class TaskA(RemoveCorruptedOutputMixin, luigi.Task):
        """TaskA can remove its output upon failure."""
        pass

```

The `clean_failed` is `false` by default and it must explicitly be set to `true`.
This allows users to set it to false to debug the output without changing the code.

```bash
luigi -m my_module TaskA --clean_failed true
```

### Copy parameters

In some situations, several tasks have a few parameters in common. This can lead to painful
situations, and luigi provides some dedicated tools to deal with this,
[as described here](https://luigi.readthedocs.io/en/stable/api/luigi.util.html?highlight=inherits#using-inherits-and-requires-to-ease-parameter-pain).
Nevertheless, the tools provided by [Luigi][luigi_url] have a major drawback: all the tasks with
the inherited parameter will have the same value for this parameter. In some situations, one want
to be able to give different values to a task with an inherited parameter, especially during the
development process. This is possible with the `@copy_params` decorator:

```python
from luigi.task import Task
from luigi_tools.task import copy_params

class TaskA(Task):
        a = luigi.Parameter(default="default_value_a")

@luigi_tools.task.copy_params(
    a=luigi_tools.task.ParamRef(TaskA)
)
class TaskB(Task):
    b = luigi.Parameter(default="b")
```

Here the class `TaskB` has two parameters:
* `a` with `default_value_a` as default value.
* `b` with `b` as default value.

It also possible to change the name of the parameter or to change the default value:

```python
from luigi.task import Task
from luigi_tools.task import copy_params

class TaskA(Task):
        a = luigi.Parameter(default="default_value_a")

@luigi_tools.task.copy_params(
    a=luigi_tools.task.ParamRef(TaskA),
    aa=luigi_tools.task.ParamRef(TaskA, "a"),
    a_default=luigi_tools.task.ParamRef(TaskA, "a", "given_default_value"),
    a_none=luigi_tools.task.ParamRef(TaskA, "a", None),
)
class TaskB(Task):
    b = luigi.Parameter(default="b")
```

In this case the class `TaskB` has 5 parameters:
* `a` with `default_value_a` as default value.
* `aa` with `a` as default value.
* `a_default` with `given_default_value` as default value.
* `a_none` with `None` as default value.
* `b` with `b` as default value.

Note that the second parameter of `ParamRef` is the name of the inherited parameter in the parent
class. If it is not given, it is supposed that the parameter has the same name in both the
inheriting and the parent classes.

### Global parameters

In addition to the `@copy_params` decorator, it is possible to use the `GlobalParamMixin` mixin.
A task with this mixin has a new feature for the parameters inherited using `@copy_params`: if the
default value is not changed in `ParamRef` and if no specific value is given for the task, then the
task would take the same value as one of the inherited parameter. This combination of the
`@copy_params` decorator and `GlobalParamMixin` allows many ways of dealing with the parameters.

```python
from luigi.task import Task
from luigi_tools.task import copy_params
from luigi_tools.task import GlobalParamMixin

class TaskA(Task):
        a = luigi.Parameter(default="default_value_a")

@luigi_tools.task.copy_params(
    a=luigi_tools.task.ParamRef(TaskA)
)
class TaskB(GlobalParamMixin, Task):
    b = luigi.Parameter(default="b")
```

Now if `TaskB` is called with the following configuration:

```yaml
[TaskA]
a = "value for a"

[TaskB]
b = "value for b"
```

then the parameter `a` of `TaskB` has the value `value for a`.
If `TaskB` did not inherit from `GlobalParamMixin`, then it would have the value
`default_value_a`.

### Dependency graph

The `luigi-tools` package provides several functions to get the dependency graph of a task and to
render it using GraphViz. This can be very useful to show how the tasks of a workflow are
organized.


## Funding & Acknowledgment

The development of this software was supported by funding to the Blue Brain Project, a research center of the École polytechnique fédérale de Lausanne (EPFL), from the Swiss government’s ETH Board of the Swiss Federal Institutes of Technology.

For license and authors, see `LICENSE.txt` and `AUTHORS.md` respectively.

Copyright © 2021-2022 Blue Brain Project/EPFL

[luigi_url]: https://luigi.readthedocs.io/en/stable/
[luigi_tools_url]: https://luigi-tools.readthedocs.io/en/stable/
[luigi_tools_api_url]: https://luigi-tools.readthedocs.io/en/stable/api.html
[snakemake_url]: https://snakemake.readthedocs.io/en/stable/
