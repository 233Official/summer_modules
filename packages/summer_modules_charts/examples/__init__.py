"""Helpers for charts examples."""

from pathlib import Path

from summer_modules_core import load_config

EXAMPLES_ROOT = Path(__file__).resolve().parent
PACKAGE_DIR = EXAMPLES_ROOT.parent


def get_example_config(section: str):
    return load_config(section, package_root=PACKAGE_DIR)


__all__ = ["get_example_config", "EXAMPLES_ROOT"]
