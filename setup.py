"""This package provides tools to extend the luigi library.

Copyright 2021 Blue Brain Project / EPFL

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
from setuptools import setup, find_packages

# Read the contents of the README file
with open("README.md", encoding="utf-8") as f:
    README = f.read()

reqs = [
    "luigi",
]

docs_reqs = [
    "m2r2",
    "mistune<2",
    "sphinx",
    "sphinx-bluebrain-theme",
]

setup(
    name="luigi-tools",
    author="Blue Brain Project, EPFL",
    description="Tools to work with luigi",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://luigi-tools.readthedocs.io/en/latest",
    project_urls={
        "Tracker": "https://github.com/BlueBrain/luigi-tools/issues",
        "Source": "https://github.com/BlueBrain/luigi-tools",
    },
    license="Apache License, Version 2.0",
    packages=find_packages(exclude=["tests"]),
    python_requires=">=3.6",
    install_requires=reqs,
    extras_require={
        "graphviz": ["graphviz"],
        "docs": docs_reqs,
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Scientific/Engineering",
    ],
    include_package_data=True,
)
