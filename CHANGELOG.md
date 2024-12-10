# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [PEP 440](https://www.python.org/dev/peps/pep-0440/)
and uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.1]

The initial release of this library. This library provides:

1. Enumeration of DIST-S1-ALERT products. A DIST-S1-ALERT product can be uniquely identified (assuming a pre-image selection process is fixed)by:
   + MGRS tile
   + Acquisition time of the post-image
   + Track of the post-image
2. Ability to localize OPERA RTC-S1 data for the creation of the DIST-S1 product.