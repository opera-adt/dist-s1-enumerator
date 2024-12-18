import geopandas as gpd
from pandera import Column, DataFrameSchema, Timestamp
from pandera.engines.pandas_engine import DateTime


burst_schema = DataFrameSchema(
    {
        'jpl_burst_id': Column(str, required=True),
        'track_number': Column(int, required=False),
        'acq_group_id_within_mgrs_tile': Column(int, required=False),
        'mgrs_tile_id': Column(str, required=False),
        'geometry': Column('geometry', required=True),
    }
)

mgrs_tile_schema = DataFrameSchema(
    {
        'mgrs_tile_id': Column(str, required=True),
        'utm_epsg': Column(int, required=True),
        'utm_wkt': Column(str, required=True),
        'geometry': Column('geometry', required=True),
    }
)

rtc_s1_schema = DataFrameSchema(
    {
        'opera_id': Column(str, required=True),
        'jpl_burst_id': Column(str, required=True),
        'acq_dt': Column(DateTime(tz='UTC'), required=True),
        'acq_date': Column(DateTime(tz='UTC'), required=False),
        'polarizations': Column(str, required=True),
        'track_number': Column(int, required=True),
        'url_crosspol': Column(str, required=True),
        'url_copol': Column(str, required=True),
        'geometry': Column('geometry', required=True),
    }
)

dist_s1_input_schema = rtc_s1_schema.add_columns(
    {
        'acq_group_id_within_mgrs_tile': Column(int, required=False),
        'track_token': Column(str, required=True),
        'mgrs_tile_id': Column(str, required=False),
        'product_id': Column(str, required=False),
        'pass_id': Column(str, required=False),
        'geometry': Column('geometry', required=True),
    }
)

burst_mgrs_lut_schema = DataFrameSchema(
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
