from pathlib import Path

import papermill as pm
import pytest


repo_dir = Path(__file__).parent.parent
notebooks_dir = repo_dir / 'notebooks'

notebooks_fns = ['A__Staging_Inputs_for_One_MGRS_Tile.ipynb', 'B__Enumerate_MGRS_tile.ipynb']


@pytest.mark.integration
@pytest.mark.parametrize('notebook_fn', notebooks_fns)
def test_notebook(notebook_fn: str) -> None:
    tmp_dir = Path(__file__).parent / 'tmp'
    tmp_dir.mkdir(parents=True, exist_ok=True)
    notebook_path = notebooks_dir / notebook_fn
    if not notebook_path.exists():
        pytest.skip(f'Notebook {notebook_fn} not found in notebooks directory')

    # Create output path in temporary directory
    output_path = Path(tmp_dir) / f'out_{notebook_fn}'

    # Execute notebook
    pm.execute_notebook(
        str(notebook_path),
        str(output_path),
        kernel_name='dist-s1-enumerator',
    )

    # cleanup
    for item in tmp_dir.iterdir():
        item.unlink()
    tmp_dir.rmdir()
