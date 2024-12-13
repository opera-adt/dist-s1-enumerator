# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [PEP 440](https://www.python.org/dev/peps/pep-0440/)
and uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.3] - 2024-12-12

### Added
* Suite of tests for enumeration

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