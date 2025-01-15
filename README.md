# dist-s1-enumerator

[![PyPI license](https://img.shields.io/pypi/l/dist-s1-enumerator.svg)](https://pypi.python.org/pypi/dist-s1-enumerator/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/dist-s1-enumerator.svg)](https://pypi.python.org/pypi/dist-s1-enumerator/)
[![PyPI version](https://img.shields.io/pypi/v/dist-s1-enumerator.svg)](https://pypi.python.org/pypi/dist-s1-enumerator/)
[![Conda version](https://img.shields.io/conda/vn/conda-forge/dist_s1_enumerator)](https://anaconda.org/conda-forge/dist_s1_enumerator)
[![Conda platforms](https://img.shields.io/conda/pn/conda-forge/dist_s1_enumerator)](https://anaconda.org/conda-forge/dist_s1_enumerator)

This is a Python library for enumerating OPERA RTC-S1 inputs necessary for the creation of OPERA DIST-S1 products.
The library can enumerate inputs for the creation of a single DIST-S1 product or a time-series of DIST-S1 products over a large area spanning multiple passes.
The DIST-S1 measures disturbance comparing a baseline of RTC-S1 images (pre-images) to a current set of acquisition images (post-images).
This library also provides functionality for downloading the OPERA RTC-S1 data from ASF DAAC.


## Installation/Setup

We recommend managing dependencies and virutal environments using [mamba/conda](https://mamba.readthedocs.io/en/latest/user_guide/installation.html).

```bash
mamba update -f environment.yml  # creates a new environment dist-s1-enumerator
conda activate dist-s1-enumerator
pip install dist-s1-enumerator
python -m ipykernel install --user --name dist-s1-enumerator
```

### Downloading data

For searching through the metadata of OPERA RTC-S1, you will not need any earthdata credentials.
For downloading data from the ASF DAAC, you will need to make sure you have a Earthdata credentials (see: https://urs.earthdata.nasa.gov/) and successfully accepted the ASF terms of use (this can be checked by downloading any product at the ASF DAAC using your Earthdata credentials: https://search.asf.alaska.edu/).
You will need to create or append to `~/.netrc` file with these credentials:
```
machine urs.earthdata.nasa.gov
    login <your_username>
    password <your_password>
```

### Development installation

Same as above replacing `pip install dist-s1-enumerator` with `pip install -e .`.

## Usage

See the [Jupyter notebooks](./notebooks) for examples.

- [Enumerating inputs for a single DIST-S1 product](./notebooks/A__Staging_Inputs_for_One_MGRS_Tile.ipynb)
- [Enumerating inputs for a time-series of DIST-S1 products](./notebooks/B__Enumerate_MGRS_tile.ipynb)

These notebooks provide discussion about how we curate OPERA RTC-S1 inputes for the creation of DIST-S1 products.

### Identifiers for DIST-S1 products

Of course, knowing all the OPERA RTC-S1 products (pre-images and post-images) necessary for a DIST-S1 product uniquely identifies the products.
However, this can be upwards of 100 products for each DIST-S1 products and is not human parsable.
Thus, it is helpful to know alterate ways to identify and trigger the DIST-S1 product and its' workflow.

Altenrately, we can uniqely identify a DIST-S1 product via its:

1. MGRS Tile ID
2. Track Number
3. Post-image acquisition time (within 1 day)

Each DIST-S1 product is resampled to an MGRS tile, thus explaining 1.
One might assume that the post-image acquisition time is enough - however, there are particular instances when Sentinel-1 A and Sentinel-1 C will pass each other in the same day and so fixing the track number differentiates between the two sets of imagery; each satellite will collect data from different geometries and thus provide imagery in different fixed spatial bursts.
Thus, it is important to specify the date in addition to the track number.
It is also important to note that we are assuming the selection of pre-images (once a post-image set is selected) is fixed.
Indeed, varying a baseline of pre-images by which to measure disturbance will alter the final DIST-S1 product.
While we can modify strategies of pre-image selection using this library, it is not highlighted here. 


# Testing

For the test suite:

1. Install `papermill` via `conda-forge` (currently not supported by 3.13)
2. Run `pytest tests`

There are two category of tests: unit tests and integration tests. The former can be run using `pytest tests -m 'not integration'` and similarly the latter with `pytest tests -m 'integration'`. The intgeration tests are those that can be integrated into the DAAC data access workflows and thus require internet access with earthdata credentials setup correctly (as described above). The unit tests mock the necessary data inputs.
The integration tests that are the most time consuming are represented by the notebooks and are run only upon a release PR.
These notebook tests are tagged with `notebooks` and can be excluded from the other tests with `pytest tests -m 'not notebooks'`.

# Contributing

We welcome contributions to this open-source package. To do so:

1. Create an GitHub issue ticket desrcribing what changes you need (e.g. issue-1)
2. Fork this repo
3. Make your modifications in your own fork
4. Make a pull-request (PR) in this repo with the code in your fork and tag the repo owner or a relevant contributor.

We use `ruff` and associated linting packages to ensure some basic code quality (see the `environment.yml`). These will be checked for each commit in a PR. Try to write tests wherever possible.

# Support

1. Create an GitHub issue ticket desrcribing what changes you would like to see or to report a bug.
2. We will work on solving this issue (hopefully with you).

# Acknowledgements

See the [LICENSE](LICENSE.txt) file for copyright information.

This package was developed as part of the Observational Products for End-Users from Remote Sensing Analysis ([OPERA](https://www.jpl.nasa.gov/go/opera/)) project.  This work was originally carried out at the Jet Propulsion Laboratory, California Institute of Technology, under a contract with the National Aeronautics and Space Administration (80NM0018D0004). 
Copyright 2024 by the California Institute of Technology. United States Government Sponsorship acknowledged.
