import shutil
from collections.abc import Callable
from pathlib import Path

import pytest


repo_dir = Path(__file__).parent.parent
notebooks_dir = repo_dir / 'notebooks'
notebooks_filenames = [
    # Ignore because of downloading
    # 'A__Staging_Inputs_for_One_MGRS_Tile.ipynb',
    'B__Enumerate_MGRS_tile.ipynb',
]
notebooks_paths = [(notebooks_dir / filename).resolve() for filename in notebooks_filenames]


@pytest.mark.integration
@pytest.mark.notebooks
@pytest.mark.parametrize('notebook_path', notebooks_paths)
def test_notebook(notebook_path: str, change_local_dir: Callable[[Path], Path], test_dir: Path) -> None:
    # TODO: move to top when papermill supports 3.13
    import papermill as pm

    # Changes the working directory to the test directory
    change_local_dir(test_dir)

    tmp_dir = test_dir / 'tmp'
    tmp_dir.mkdir(parents=True, exist_ok=True)
    if not notebook_path:
        pytest.skip(f'Notebook {notebook_path} not found in notebooks directory')

    # Create output path in temporary directory
    output_path = Path(tmp_dir) / f'out_{notebook_path.name}'

    # Execute notebook
    pm.execute_notebook(
        str(notebook_path),
        str(output_path),
        kernel_name='dist-s1-enumerator',
    )

    # cleanup
    cleanup_dirs = [
        tmp_dir,
        # # output directory of 'A__Staging_Inputs_for_One_MGRS_Tile.ipynb'
        # test_dir / 'out',
    ]
    [shutil.rmtree(dir) for dir in cleanup_dirs]
