# dist-s1-enumerator

[![Actions Status][actions-badge]][actions-link]
[![PyPI version][pypi-version]][pypi-link]

[![Conda-Forge][conda-badge]][conda-link]
[![GitHub Discussion][github-discussions-badge]][github-discussions-link]


<!-- prettier-ignore-start -->
[actions-badge]:            https://github.com/opera-adt/dist-s1-enumerator/workflows/CI/badge.svg
[actions-link]:             https://github.com/opera-adt/dist-s1-enumerator/actions
[conda-badge]:              https://img.shields.io/conda/vn/conda-forge/dist-s1-enumerator
[conda-link]:               https://github.com/conda-forge/dist-s1-enumerator-feedstock
[github-discussions-badge]: https://img.shields.io/static/v1?label=Discussions&message=Ask&color=blue&logo=github
[github-discussions-link]:  https://github.com/opera-adt/dist-s1-enumerator/discussions
[pypi-link]:                https://pypi.org/project/dist-s1-enumerator/
[pypi-platforms]:           https://img.shields.io/pypi/pyversions/dist-s1-enumerator
[pypi-version]:             https://img.shields.io/pypi/v/dist-s1-enumerator

<!-- prettier-ignore-end -->

This is a Python library for staging data necessary for the DIST-S1 workflow.

## Installation

```bash
mamba update -f environment.yml
pip install -e .
conda activate dist-s1-env
python -m ipykernel install --user --name dist-s1-env
```

## Usage

See the [Jupyter notebooks](./notebooks) for examples.