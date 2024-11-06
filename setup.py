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

"""Setup for the luigi-tools package."""

from pathlib import Path

from setuptools import find_namespace_packages
from setuptools import setup

reqs = [
    "luigi>=3.1",
    "jsonschema>=4.2",
    "typing-extensions>=4",
]

doc_reqs = [
    "docutils<0.21",  # Temporary fix for m2r2
    "m2r2",
    "sphinx",
    "sphinx-bluebrain-theme",
]

test_reqs = [
    "mock>=3",
    "pytest>=8",
    "pytest-cov>=4.1",
    "pytest-html>=4",
]

setup(
    name="luigi-tools",
    author="Blue Brain Project, EPFL",
    description="Tools to work with luigi.",
    long_description=Path("README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    url="https://luigi-tools.readthedocs.io",
    project_urls={
        "Tracker": "https://github.com/BlueBrain/luigi-tools/issues",
        "Source": "https://github.com/BlueBrain/luigi-tools",
    },
    license="Apache License 2.0",
    packages=find_namespace_packages(include=["luigi_tools*"]),
    python_requires=">=3.9",
    use_scm_version=True,
    setup_requires=[
        "setuptools_scm",
    ],
    install_requires=reqs,
    extras_require={
        "docs": doc_reqs,
        "graphviz": ["graphviz"],
        "test": test_reqs,
    },
    include_package_data=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering",
    ],
)
