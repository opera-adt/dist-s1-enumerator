from pathlib import Path

import geopandas as gpd

from dist_s1_enumerator.dist_enum_inputs import enumerate_dist_s1_workflow_inputs


def test_enumerate_dist_s1_workflow_inputs_for_time_series(test_dir: Path) -> None:
    expected_output = [
        {'mgrs_tile_id': '19HBD', 'post_acq_date': '2023-11-05', 'track_number': 91},
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
        {'mgrs_tile_id': '19HBD', 'post_acq_date': '2024-03-28', 'track_number': 91},
    ]

    # Chile 19HBD
    df_ts = gpd.read_parquet(test_dir / 'data' / 'rtc_s1_ts_metadata' / 'chile_19HBD.parquet')

    workflow_inputs = enumerate_dist_s1_workflow_inputs(
        mgrs_tile_ids='19HBD',
        track_numbers=None,
        start_acq_dt='2023-11-01',
        stop_acq_dt='2024-04-01',
        lookback_strategy='multi_window',
        delta_lookback_days=365,
        max_pre_imgs_per_burst=5,
        df_ts=df_ts,
    )

    assert workflow_inputs == expected_output
