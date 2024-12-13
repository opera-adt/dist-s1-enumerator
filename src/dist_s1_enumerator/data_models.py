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
