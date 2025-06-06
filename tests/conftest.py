import os
from collections.abc import Callable, Generator
from pathlib import Path

import pytest


@pytest.fixture(scope='session')
def change_local_dir() -> Generator[Callable[[Path], Path], None, None]:
    """Fixture to temporarily change the working directory."""
    original_dir = Path.cwd()

    def _change_dir(target_dir: Path) -> Path:
        target_dir = Path(target_dir).resolve()
        os.chdir(target_dir)
        return target_dir

    yield _change_dir

    # Restore the original directory
    os.chdir(original_dir)
    assert Path.cwd() == original_dir


@pytest.fixture(scope='session')
def test_dir() -> Path:
    """Fixture to provide the path to the test directory."""
    test_dir = Path(__file__).parent
    test_dir = test_dir.resolve()
    return test_dir
