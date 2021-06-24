#!/usr/bin/env python3

import imp
import sys

from setuptools import setup, find_packages

if sys.version_info < (3, 6):
    sys.exit("Sorry, Python < 3.6 is not supported")

# Read the contents of the README file
with open("README.rst", encoding="utf-8") as f:
    README = f.read()

reqs = [
    "luigi",
]

docs_reqs = [
    "m2r2",
    "sphinx",
    "sphinx-bluebrain-theme",
]

VERSION = imp.load_source("", "luigi_tools/version.py").VERSION

setup(
    name="luigi-tools",
    author="bbp-ou-nse",
    author_email="bbp-ou-nse@groupes.epfl.ch",
    version=VERSION,
    description="Tools to work with luigi",
    long_description=README,
    long_description_content_type="text/x-rst",
    url="https://bbpteam.epfl.ch/documentation/projects/luigi-tools",
    project_urls={
        "Tracker": "https://github.com/BlueBrain/luigi-tools/issues",
        "Source": "https://github.com/BlueBrain/luigi-tools",
    },
    license="BBP-internal-confidential",
    packages=find_packages(exclude=["tests"]),
    python_requires=">=3.6",
    install_requires=reqs,
    extras_require={
        "graphviz": ["graphviz"],
        "docs": docs_reqs,
    },
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
    ],
    include_package_data=True,
)
