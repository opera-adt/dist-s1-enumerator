import geopandas as gpd
import pandas as pd
from pandera import Column, DataFrameSchema


BURST_SCHEMA = DataFrameSchema(
    {
        'jpl_burst_id': Column(str, required=True),
        'track_number': Column(int, required=False),
        'acq_group_id_within_mgrs_tile': Column(int, required=False),
        'mgrs_tile_id': Column(str, required=False),
        'geometry': Column('geometry', required=True),
    }
)

MGRS_TILE_SCHEMA = DataFrameSchema(
    {
        'mgrs_tile_id': Column(str, required=True),
        'utm_epsg': Column(int, required=True),
        'utm_wkt': Column(str, required=True),
        'geometry': Column('geometry', required=True),
    }
)

RTC_S1_SCHEMA = DataFrameSchema(
    {
        'opera_id': Column(str, required=True),
        'jpl_burst_id': Column(str, required=True),
        'acq_dt': Column(pd.Timestamp, required=True),
        'polarizations': Column(str, required=True),
        'track_number': Column(int, required=True),
        'url_crosspol': Column(str, required=True),
        'url_copol': Column(str, required=True),
        'loc_path_crosspol': Column(str, required=False),
        'loc_path_copol': Column(str, required=False),
        'acq_group_id_within_mgrs_tile': Column(int, required=False),
        'mgrs_tile_id': Column(str, required=False),
        'geometry': Column('geometry', required=True),
    }
)

BURST_MGRS_LUT_SCHEMA = DataFrameSchema(
    {
        'jpl_burst_id': Column(str, required=True),
        'mgrs_tile_id': Column(str, required=True),
        'track_number': Column(int, required=True),
        'acq_group_id_within_mgrs_tile': Column(int, required=True),
        'orbit_pass': Column(str, required=True),
        'area_per_acq_group_km2': Column(int, required=True),
        'n_bursts_per_acq_group': Column(int, required=True),
    }
)


def reorder_columns(df: gpd.GeoDataFrame, schema: DataFrameSchema) -> gpd.GeoDataFrame:
    df = df[[col for col in schema.columns.keys() if col in df.columns]]
    return df
