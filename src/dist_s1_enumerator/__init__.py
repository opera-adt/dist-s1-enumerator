import warnings
from importlib.metadata import PackageNotFoundError, version

from .asf import (
    agg_rtc_metadata_by_burst_id,
    get_rtc_s1_ts_metadata,
)
from .mgrs_burst_data import (
    get_burst_ids_in_mgrs_tiles,
    get_burst_table,
    get_burst_table_from_mgrs_tiles,
    get_lut_by_mgrs_tile_ids,
    get_mgrs_burst_lut_path,
    get_mgrs_table,
    get_mgrs_tiles_overlapping_geometry,
)


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


__all__ = [
    'get_mgrs_table',
    'get_burst_table',
    'get_lut_by_mgrs_tile_ids',
    'get_burst_table_from_mgrs_tiles',
    'get_burst_ids_in_mgrs_tiles',
    'get_mgrs_tiles_overlapping_geometry',
    'get_rtc_s1_ts_metadata',
    'get_mgrs_burst_lut_path',
    'agg_rtc_metadata_by_burst_id',
]
