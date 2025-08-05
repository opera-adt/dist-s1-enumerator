import warnings
from importlib.metadata import PackageNotFoundError, version

import asf_search

from dist_s1_enumerator.asf import (
    agg_rtc_metadata_by_burst_id,
    get_rtc_s1_metadata_from_acq_group,
    get_rtc_s1_ts_metadata_from_mgrs_tiles,
)
from dist_s1_enumerator.dist_enum import enumerate_dist_s1_products, enumerate_one_dist_s1_product
from dist_s1_enumerator.dist_enum_inputs import enumerate_dist_s1_workflow_inputs
from dist_s1_enumerator.mgrs_burst_data import (
    get_burst_ids_in_mgrs_tiles,
    get_burst_table,
    get_burst_table_from_mgrs_tiles,
    get_lut_by_mgrs_tile_ids,
    get_mgrs_burst_lut,
    get_mgrs_burst_lut_path,
    get_mgrs_table,
    get_mgrs_tiles_overlapping_geometry,
)
from dist_s1_enumerator.rtc_s1_io import localize_rtc_s1_ts


try:
    __version__ = version(__name__)
except PackageNotFoundError:
    __version__ = None
    warnings.warn(
        'package is not installed!\n'
        'Install in editable/develop mode via (from the top of this repo):\n'
        '   python -m pip install -e .\n',
        RuntimeWarning,
    )
# Increase CMR timeout to 2 minutes
asf_search.constants.INTERNAL.CMR_TIMEOUT = 120

__all__ = [
    'agg_rtc_metadata_by_burst_id',
    'agg_rtc_metadata_by_burst_id',
    'enumerate_dist_s1_products',
    'enumerate_dist_s1_products',
    'enumerate_dist_s1_workflow_inputs',
    'enumerate_one_dist_s1_product',
    'get_burst_ids_in_mgrs_tiles',
    'get_burst_table_from_mgrs_tiles',
    'get_burst_table',
    'get_lut_by_mgrs_tile_ids',
    'get_mgrs_burst_lut',
    'get_mgrs_burst_lut_path',
    'get_mgrs_table',
    'get_mgrs_tiles_overlapping_geometry',
    'get_rtc_s1_metadata_from_acq_group',
    'get_rtc_s1_ts_metadata_from_mgrs_tiles',
    'localize_rtc_s1_ts',
]
