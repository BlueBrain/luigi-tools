"""A few helpers for tests"""
from pathlib import Path


def check_existing_file(filename):
    """Check if a file exists"""
    return Path(filename).exists()


def create_empty_file(filename):
    """Create an empty file"""
    with open(filename, "w"):
        pass


def check_empty_file(filename):
    """Checck that a file is empty"""
    with open(filename) as f:
        return f.read() == ""


def create_not_empty_file(filename):
    """Create a not empty file"""
    with open(filename, "w") as f:
        f.write("NOT EMPTY")


def check_not_empty_file(filename):
    """Checck that a file is not empty"""
    with open(filename) as f:
        return f.read() == "NOT EMPTY"
