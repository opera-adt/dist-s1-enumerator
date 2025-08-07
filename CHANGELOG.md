# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [PEP 440](https://www.python.org/dev/peps/pep-0440/)
and uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1] - 2025-08-07

### Added
* Improved tqdm outputs for enumeration of 1 dist-s1 product with multiwindow
* Added back more print statements

## [1.0.0] - 2025-08-05

### Changed
- `multi_window` is now the default parameter
- Consolidated `*_mw` and other parameters for single API.
- Assumes 3 anniversary dates for multi_window strategy

### Fixed
- New pandera import works only on 0.24.0
- Allow for acquisition metadata to be obtained around anniversary date 
- Typing of functions.

### Removed
- print statements

### Added
- `reorder_columns` can accept empty dataframes and creates empty table with schema (important for longer number of pre-image anniversaries), where there may be no acquisitions available.
- More docstring information on the enumeration functions
- pydantic models for updating/validating with `immediate_lookback` and `multi_window` strategy
- Function to get inputs for triggering `dist-s1` workflow (not necessarily all the RTC input data).
- Tests for multi-window lookback strategy
- Tests for workflow inputs


## [0.0.9] - 2025-06-06

### Fixed
- Pandera imports are changing and currently raising lots of warnings. We resolve these warnings.

## [0.0.8] - 2025-05-27

### Added
* Implemented `lookback_strategy` in `dist_enum`
* Use option `immediate_lookback` to search for the pre dates immediatelly before the `post_date`
* Use option `multi_window` to search for pre dates as windows defined by `max_pre_imgs_per_burst_mw` and `delta_lookback_days_mw` 


## [0.0.7] - 2025-01-16

### Fixed
* Fixed bug where RTC-S1 data was not being sorted by `jpl_burst_id` and `acq_dt` - this was causing us to establish baseline imagery from incorrect position


## [0.0.6] - 2025-01-16
  
## Changed
* Renamed `disable_tqdm` to `tqdm_enabled` in `localize_rtc_s1_ts`

## Added
* Description to `tqdm` progress bar in `localize_rtc_s1_ts`.
* Use `tqdm.auto` to automatically determine how `tqdm` should be displayed (either via CLI or in a notebook).

## [0.0.5] - 2025-01-16

* Dummy release due to expired github token.

## [0.0.4] - 2024-12-30

### Removed
* Removed `papermill` from environment.yml as it is not supported by 3.13

### Added
* Added `localize_rtc_s1_ts` to top-level imports
* Allowed `post_date` to be a string in the form of 'YYYY-MM-DD' for one product enumeration
* Schema for localized inputs to ensure columns for local paths: `loc_path_copol` and `loc_path_crosspol`

### Changed
* Added print date in notebook for clarity.
* Remove schema check from `append_pass_data` and enforce only certain columns to be present. 


## [0.0.3] - 2024-12-12

### Added
* Support for Python 3.13
* Explicit error messages when no data is retrieved from various tables (e.g. burst data, MGRS/burst LUT data, etc.)
* Suite of tests for enumeration
   * Unit tests - tests that can be run in a quick fashion and will be run on each PR to main/dev
   * Integration tests - in our case, hitting the DAAC API and downloading data when necessary; these also include running the Notebooks.
   * The latter is marked with `@pytest.mark.integration` and will be run on PRs to main (i.e. release PRs)
* Schema with Pandera to explicitly define and validate columns and their types
* Flexibility to retrieve either HH+HV or VV+VH or target one particular dual polarization type (currently does not support mixtures of dual polarized data).
* Expose the primary functions via the `__init__.py` file
* Updated environment.yml for Papermill tests

## Fixed
* `epsg` (now `utm_epsg`) was a string (with extra spacing) and now it's an integer

## Changed
* For the MGRS table, we renamed `epsg` to `utm_epsg` (to be in line with `utm_wkt`) and cast it as an int

## [0.0.2]

### Added
1. Minimum working example of enumeration of DIST-S1 products from MGRS tiles and (optionally) a track number in said MGRS tiles.

### Changed
API is *very* much in flux to support enumeration of large spatial areas and to support the localization of RTC-S1 data. Will likely continue to change to promote changing.

## [0.0.1]

The initial release of this library. This library provides:

1. Enumeration of DIST-S1-ALERT products. A DIST-S1-ALERT product can be uniquely identified (assuming a pre-image selection process is fixed)by:
   + MGRS tile
   + Acquisition time of the post-image
   + Track of the post-image
2. Ability to localize OPERA RTC-S1 data for the creation of the DIST-S1 product.
