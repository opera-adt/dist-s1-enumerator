from __future__ import annotations

import importlib.metadata

import dist_s1_enumerator as package


def test_version() -> None:
    assert importlib.metadata.version('dist_s1_enumerator') == package.__version__
