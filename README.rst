|build| |license| |docs|

Luigi tools
===========

This project aims to bring together the luigi tools used within the BBP.

Installation
------------

This package should be installed using pip:

.. code-block:: bash

    pip install luigi-tools

Usage
-----

This package extends and adds new features to the [luigi package](https://luigi-tools.readthedocs.io/en/stable/).
Here are a few examples of these features:

* add a new `BoolParameter` that automatically switch to explicit parsing when the default value is `True` (otherwise it is not possible to set it to `False` using the CLI).
* add several types of optional parameters.
* add a `OutputLocalTarget` class to help building an output tree.
* add a mixin that adds a `--rerun` parameter that forces a given task to run again even if its targets exist, and also rerun all the tasks that depend on this one.
* add a new `@copy_params` mechanism to copy the parameters from a task to another (the `@inherits` gives the same object to all the inheriting tasks while `@copy_params` only copies the definition of the parameter so each inheriting task can be given a different value).
* add functions to get and display the dependency graph of a given task.
* add a mechanism to setup templates for the `luigi.cfg` files, so the user just has to update specific values instead of copying the entire `luigi.cfg`.

Please read the complete documentation for more details: https://bbpteam.epfl.ch/documentation/projects/luigi-tools


Copyright © 2021 Blue Brain Project/EPFL

.. |license| image:: https://img.shields.io/aur/license/BlueBrain/luigi-tools
   :target: https://github.com/BlueBrain/luigi-tools/blob/master/LICENSE.txt
   :alt: License

.. |build| image:: https://github.com/BlueBrain/luigi-tools/actions/workflows/run-tox.yml/badge.svg?branch=main
   :target: https://github.com/BlueBrain/luigi-tools/actions
   :alt: Build Status

.. |docs| image:: https://readthedocs.org/projects/luigi-tools/badge/?version=latest
   :target: https://luigi-tools.readthedocs.io/
   :alt: Documentation status
