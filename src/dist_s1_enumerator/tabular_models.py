import geopandas as gpd
from pandera.engines.pandas_engine import DateTime
from pandera.pandas import Column, DataFrameSchema


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

# Response schema from ASF DAAC API
rtc_s1_resp_schema = DataFrameSchema(
    {
        'opera_id': Column(str, required=True),
        'jpl_burst_id': Column(str, required=True),
        'acq_dt': Column(DateTime(tz='UTC'), required=True),
        'acq_date_for_mgrs_pass': Column(str, required=False),
        'polarizations': Column(str, required=True),
        'track_number': Column(int, required=True),
        # Integer number of 6 day periods since 2014-01-01
        'pass_id': Column(int, required=True),
        'url_crosspol': Column(str, required=True),
        'url_copol': Column(str, required=True),
        'geometry': Column('geometry', required=True),
    }
)

# Schema for RTC-S1 metadata with MGRS tile and acq group id appended
# Note: a single burst product may be associated with multiple MGRS tiles and acq group_ids
rtc_s1_schema = rtc_s1_resp_schema.add_columns(
    {
        'mgrs_tile_id': Column(str, required=True),
        'acq_group_id_within_mgrs_tile': Column(int, required=True),
        'track_token': Column(str, required=True),
        'geometry': Column('geometry', required=True),
    }
)

# Schema for inputs to dist-s1 workflow
dist_s1_input_schema = rtc_s1_schema.add_columns(
    {
        'input_category': Column(str, required=True),
        'product_id': Column(int, required=False),
        'geometry': Column('geometry', required=True),
    }
)

# Schema for localized inputs
dist_s1_loc_input_schema = dist_s1_input_schema.add_columns(
    {
        'loc_path_copol': Column(str, required=True),
        'loc_path_crosspol': Column(str, required=True),
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
    if not df.empty:
        df = df[[col for col in schema.columns.keys() if col in df.columns]]
    else:
        df = gpd.GeoDataFrame(columns=schema.columns.keys())
        if 'geometry' in schema.columns.keys():
            df.set_crs(epsg=4326)
    return df
