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

"""A few helpers for luigi-tools test suite."""
from pathlib import Path


def check_existing_file(filename):
    """Check if a file exists."""
    return Path(filename).exists()


def create_empty_file(filename):
    """Create an empty file."""
    with open(filename, "w", encoding="utf-8"):
        pass


def check_empty_file(filename):
    """Check that a file is empty."""
    with open(filename, encoding="utf-8") as f:
        return f.read() == ""


def create_not_empty_file(filename):
    """Create a not empty file."""
    with open(filename, "w", encoding="utf-8") as f:
        f.write("NOT EMPTY")


def check_not_empty_file(filename):
    """Check that a file is not empty."""
    with open(filename, encoding="utf-8") as f:
        return f.read() == "NOT EMPTY"
