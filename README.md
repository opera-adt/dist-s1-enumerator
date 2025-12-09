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
We use "enumeration" to describe the "curation of required DIST-S1 inputs."


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

### Motivation

We want to generate a DIST-S1 product using [dist-s1](https://github.com/opera-adt/dist-s1). We successfully installed the software, but don't know how to call the CLI:

```
dist-s1 run \
    --mgrs_tile_id '19HBD' \
    --post_date '2024-03-28' \
    --track_number 91
```
Where do these inputs come from? Can we get them without looking up RTC-S1 products manually? Of course! That's the point of this library.

### Triggering the DIST-S1 Workflow

Each DIST-S1 product is uniquely identified in space and time by:

1. an MGRS Tile ID
2. a Track Number of Sentinel-1
3. the post-image acquisition time (within 1 day)

These pieces of information are required to generate any given DIST-S1 product.
Identifying all such products over time (acceptable times of the post-image) and space (MGRS tiles) allows us to enumerate all DIST-S1 products.
We can enumerate DIST-S1 products with this library as follows:
```
from dist_s1_enumerator import enumerate_dist_s1_workflow_inputs

workflow_inputs = enumerate_dist_s1_workflow_inputs(mgrs_tile_ids='19HBD',
                                                    track_numbers=None,
                                                    start_acq_dt='2023-11-01',
                                                    stop_acq_dt='2024-04-01')
```
Yields:
<details>
  <summary>Output</summary>

```[{'mgrs_tile_id': '19HBD', 'post_acq_date': '2023-11-05', 'track_number': 91},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2023-11-10', 'track_number': 156},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2023-11-12', 'track_number': 18},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2023-11-17', 'track_number': 91},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2023-11-22', 'track_number': 156},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2023-11-24', 'track_number': 18},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2023-12-04', 'track_number': 156},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2023-12-06', 'track_number': 18},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2023-12-11', 'track_number': 91},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2023-12-16', 'track_number': 156},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2023-12-18', 'track_number': 18},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2023-12-23', 'track_number': 91},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2023-12-28', 'track_number': 156},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2023-12-30', 'track_number': 18},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-01-04', 'track_number': 91},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-01-09', 'track_number': 156},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-01-11', 'track_number': 18},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-01-16', 'track_number': 91},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-01-21', 'track_number': 156},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-01-23', 'track_number': 18},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-01-28', 'track_number': 91},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-02-02', 'track_number': 156},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-02-04', 'track_number': 18},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-02-09', 'track_number': 91},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-02-14', 'track_number': 156},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-02-16', 'track_number': 18},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-02-21', 'track_number': 91},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-02-26', 'track_number': 156},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-02-28', 'track_number': 18},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-03-04', 'track_number': 91},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-03-09', 'track_number': 156},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-03-11', 'track_number': 18},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-03-16', 'track_number': 91},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-03-21', 'track_number': 156},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-03-23', 'track_number': 18},
 {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-03-28', 'track_number': 91}]
```
</details>
Each dictionary uniquely determines a DIST-S1 product.
In fact, the list above is a complete account of all DIST-S1 products over this MGRS tile and during this time period.
We can use any of the dictionaries in the list to trigger the DIST-S1 workflow e.g. using the last dictionary in the list above:

```
dist-s1 run \
    --mgrs_tile_id '19HBD' \
    --post_date '2024-03-28' \
    --track_number 91
```

See the [dist-s1](https://github.com/opera-adt/dist-s1) repository for more details on the `dist-s1` usage and workflow.

### Obtaining RTC-S1 Inputs for a given DIST-S1 product

In addition to figuring out the relevant information to trigger the DIST-S1 workflow, we can query NASA's Common Metadata Repository to identify all RTC-S1 products required to create this DIST-S1 product that are used in the workflow.
This is done above, except we only save information required to trigger the actual DIST-S1 worklow.
Here is an example to get the full account of the necessary RTC-S1 input products for a given set of DIST-S1 workflow inputs:
```
from dist_s1_enumerator import enumerate_one_dist_s1_product

df_product_t91 = enumerate_one_dist_s1_product('20TLP', track_number=[91], post_date='2025-09-25')
df_product_t91.head()
```
<details>
  <summary>Output</summary>

```opera_id     jpl_burst_id  \
0  OPERA_L2_RTC-S1_T091-193570-IW3_20240807T22192...  T091-193570-IW3   
1  OPERA_L2_RTC-S1_T091-193570-IW3_20240819T22192...  T091-193570-IW3   
2  OPERA_L2_RTC-S1_T091-193570-IW3_20240831T22192...  T091-193570-IW3   
3  OPERA_L2_RTC-S1_T091-193570-IW3_20240912T22192...  T091-193570-IW3   
4  OPERA_L2_RTC-S1_T091-193570-IW3_20240924T22192...  T091-193570-IW3   

                     acq_dt acq_date_for_mgrs_pass polarizations  \
0 2024-08-07 22:19:28+00:00             2024-08-07         VV+VH   
1 2024-08-19 22:19:28+00:00             2024-08-19         VV+VH   
2 2024-08-31 22:19:28+00:00             2024-08-31         VV+VH   
3 2024-09-12 22:19:29+00:00             2024-09-12         VV+VH   
4 2024-09-24 22:19:29+00:00             2024-09-24         VV+VH   

   track_number  pass_id                                       url_crosspol  \
0            91      645  https://cumulus.asf.earthdatacloud.nasa.gov/OP...   
1            91      647  https://cumulus.asf.earthdatacloud.nasa.gov/OP...   
2            91      649  https://cumulus.asf.earthdatacloud.nasa.gov/OP...   
3            91      651  https://cumulus.asf.earthdatacloud.nasa.gov/OP...   
4            91      653  https://cumulus.asf.earthdatacloud.nasa.gov/OP...   

                                           url_copol  \
0  https://cumulus.asf.earthdatacloud.nasa.gov/OP...   
1  https://cumulus.asf.earthdatacloud.nasa.gov/OP...   
2  https://cumulus.asf.earthdatacloud.nasa.gov/OP...   
3  https://cumulus.asf.earthdatacloud.nasa.gov/OP...   
4  https://cumulus.asf.earthdatacloud.nasa.gov/OP...   

                                            geometry mgrs_tile_id  \
0  POLYGON ((-65.58616 43.67944, -65.07523 43.740...        20TLP   
1  POLYGON ((-65.58746 43.68056, -65.07652 43.741...        20TLP   
2  POLYGON ((-65.58803 43.68023, -65.07706 43.741...        20TLP   
3  POLYGON ((-65.58995 43.68007, -65.07902 43.740...        20TLP   
4  POLYGON ((-65.5893 43.67982, -65.07838 43.7406...        20TLP   

   acq_group_id_within_mgrs_tile track_token input_category  
0                              2          91            pre  
1                              2          91            pre  
2                              2          91            pre  
3                              2          91            pre  
4                              2          91            pre  
```
</details>
The output is a pandas dataframe that can be serialized using the pandas API:

```
df_product_t91.to_csv("df_product.csv", index=False)
```

For more details see the [Jupyter notebooks](./notebooks):

- [Enumerating inputs for a single DIST-S1 product](./notebooks/A__Staging_Inputs_for_One_MGRS_Tile.ipynb)
- [Enumerating inputs for a time-series of DIST-S1 products](./notebooks/B__Enumerate_MGRS_tile.ipynb)

### Identifiers for DIST-S1 products

As noted above, each DIST-S1 product is uniquely identified by:

1. MGRS Tile ID
2. Track Number
3. Post-image acquisition time (within 1 day)

We briefly explain why these fields uniquely identify DIST-S1 products.
These pieces information uniquely describe the space (MGRS tile and track) and time (post-image acquisition) that a Sentinel-1 makes a pass over a fixed area.
Each DIST-S1 product is resampled to an MGRS tile, so we need that.
While the post-image acquisition time is a lot - there are particular instances when Sentinel-1 constellation passes over the same area in a single day and so fixing the track number differentiates between the two different sets of acquired imagery occurring in the same 24 hour period.
In theory, we could specify the exact time of acquisition, but we have elected to use track numbers to differentiate when there Sentinel-1 constellation collects data over the same area in a single day.
It is also important to note that we are assuming the selection of pre-images (once a post-image set is selected) is fixed.
Although varying a baseline of pre-images to measure disturbance will alter the final DIST-S1 product, we assume with a fixed strategy to construct this baseline, the above 3 fields uniquely identify a DIST-S1 product.

# About the Data Tables in this Library

One of the purposes of this data is to provide easy access via standard lookups to a variety of tables associated with enumerating DIST-S1 products.
There are three data tables:

1. [Burst Geometry Table](src/dist_s1_enumerator/data/jpl_burst_geo.parquet) - the JPL spatially fixed bursts within 2 km of land as identified via the UMD Ocean Mask ([link](https://console.cloud.google.com/storage/browser/earthenginepartners-hansen/OceanMask;tab=objects?prefix=&forceOnObjectsSortingFiltering=false))
2. [MGRS Table](src/dist_s1_enumerator/data/mgrs.parquet) - the MGRS tiles that are (1) used in DIST-HLS processing (see this [list](tests/data/dist_hls_tiles.txt)) and (2) have overlapping bursts from 1.
3. [MGRS/Burst Lookup Table](src/dist_s1_enumerator/data/mgrs_burst_lookup_table.parquet) - this is effectively a spatial join of burst geometries and MGRS tiles to allow us to get all relevant bursts from a pass. A pass is defined to be all the data collected over an MGRS tile from Sentinel-1, i.e. all the RTC-S1 products coming from the Sentinel-1.

How these tables were created be found in this [notebook](https://github.com/OPERA-Cal-Val/dist-s1-research/blob/dev/marshak/Zc_check_bursts_without_mgrs_tile/1__Lookup%20Tables%20for%20MGRS%20and%20Bursts.ipynb).
It's worth noting there is some care taken to do the accounting of track numbers within a Sentinel-1 acquisition to properly identify a single data take.
Sentinel-1 track numbers of products increment near the equator even though they are still within the same pass. 
Thus, we include the column `acq_group_id_within_mgrs_tile` to identify different data takes within a single MGRS tile.
We also filter out burst/mgrs pairs if the a Sentinel-1 pass that is smaller than 250 km^2 within the intersection. The MGRS tiles are 3660 x 3660 pixels at 30 meter resolution and so have total area of 12,056 km^2. Thus, this minimum overlap means if a data acquisition over an MGRS tile has less than about 2 percent of total possible data, then we do not need to create a DIST-S1 product for it. Because there is at least 10 km of overlap<sup>*</sup> between adjacent tiles (more at higher latitudes), this minimum coverage requirement means such excluded products will likely be better represented in adjacent MGRS tiles.

<sup>*</sup>Although there is [documentation](https://hls.gsfc.nasa.gov/products-description/tiling-system/) saying there is 4.9 overlap between tiles, looking at the MGRS tile table above, we see that overlap is closer to 10 km, or 9% of overlap of the area (since the MGRS tiles are about 109 km x 109 km).

# Testing

For the test suite:

1. Install `papermill` via `conda-forge` (currently not supported by 3.13)
2. Run `pytest tests`

There are two category of tests: unit tests and integration tests. The former can be run using `pytest tests -m 'not integration'` and similarly the latter with `pytest tests -m 'integration'`. The intgeration tests are those that can be integrated into the DAAC data access workflows and thus require internet access with earthdata credentials setup correctly (as described above). The unit tests mock the necessary data inputs.
The integration tests that are the most time consuming are represented by the notebooks and are run only upon a release PR.
These notebook tests are tagged with `notebooks` and can be excluded from the other tests with `pytest tests -m 'not notebooks'`.

# Remarks about the Dateline/Dateline and Geometry

The antimeridian (or dateline) is the line at the -180 longitude mark that global CRS tiles are wrapped by standard global reference systems.
The geometries of the bursts and the MGRS tiles in this package are all in `epsg:4326` (standard lon/lat). 
The geometries are all between -180 and 180 so those geometries that cross the antimeridian/dateline are generally wrapped.
For MGRS tiles, the statement that a geometry overlaps the antimeridian occurs if and only if the geometry is a Polygon.
The same is true for burst geometries.
See `test_antimeridian_crossing` in [`tests/test_mgrs_burst_data.py`](tests/test_mgrs_burst_data.py).

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
