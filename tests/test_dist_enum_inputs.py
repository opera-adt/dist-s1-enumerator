from pathlib import Path

import geopandas as gpd

from dist_s1_enumerator.dist_enum_inputs import enumerate_dist_s1_workflow_inputs


def test_enumerate_dist_s1_workflow_inputs_for_time_series(test_dir: Path) -> None:
    expected_output = [
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2023-11-05',
            'post_acq_timestamp': '2023-11-05 23:36:49+00:00',
            'track_number': 91,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2023-11-10',
            'post_acq_timestamp': '2023-11-10 10:04:33+00:00',
            'track_number': 156,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2023-11-12',
            'post_acq_timestamp': '2023-11-12 23:28:39+00:00',
            'track_number': 18,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2023-11-17',
            'post_acq_timestamp': '2023-11-17 23:36:49+00:00',
            'track_number': 91,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2023-11-22',
            'post_acq_timestamp': '2023-11-22 10:04:33+00:00',
            'track_number': 156,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2023-11-24',
            'post_acq_timestamp': '2023-11-24 23:28:39+00:00',
            'track_number': 18,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2023-12-04',
            'post_acq_timestamp': '2023-12-04 10:04:33+00:00',
            'track_number': 156,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2023-12-06',
            'post_acq_timestamp': '2023-12-06 23:28:39+00:00',
            'track_number': 18,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2023-12-11',
            'post_acq_timestamp': '2023-12-11 23:36:48+00:00',
            'track_number': 91,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2023-12-16',
            'post_acq_timestamp': '2023-12-16 10:04:32+00:00',
            'track_number': 156,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2023-12-18',
            'post_acq_timestamp': '2023-12-18 23:28:38+00:00',
            'track_number': 18,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2023-12-23',
            'post_acq_timestamp': '2023-12-23 23:36:47+00:00',
            'track_number': 91,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2023-12-28',
            'post_acq_timestamp': '2023-12-28 10:04:31+00:00',
            'track_number': 156,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2023-12-30',
            'post_acq_timestamp': '2023-12-30 23:28:37+00:00',
            'track_number': 18,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-01-04',
            'post_acq_timestamp': '2024-01-04 23:36:47+00:00',
            'track_number': 91,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-01-09',
            'post_acq_timestamp': '2024-01-09 10:04:31+00:00',
            'track_number': 156,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-01-11',
            'post_acq_timestamp': '2024-01-11 23:28:37+00:00',
            'track_number': 18,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-01-16',
            'post_acq_timestamp': '2024-01-16 23:36:46+00:00',
            'track_number': 91,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-01-21',
            'post_acq_timestamp': '2024-01-21 10:04:30+00:00',
            'track_number': 156,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-01-23',
            'post_acq_timestamp': '2024-01-23 23:28:36+00:00',
            'track_number': 18,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-01-28',
            'post_acq_timestamp': '2024-01-28 23:36:46+00:00',
            'track_number': 91,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-02-02',
            'post_acq_timestamp': '2024-02-02 10:04:30+00:00',
            'track_number': 156,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-02-04',
            'post_acq_timestamp': '2024-02-04 23:28:36+00:00',
            'track_number': 18,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-02-09',
            'post_acq_timestamp': '2024-02-09 23:36:45+00:00',
            'track_number': 91,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-02-14',
            'post_acq_timestamp': '2024-02-14 10:04:29+00:00',
            'track_number': 156,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-02-16',
            'post_acq_timestamp': '2024-02-16 23:28:36+00:00',
            'track_number': 18,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-02-21',
            'post_acq_timestamp': '2024-02-21 23:36:46+00:00',
            'track_number': 91,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-02-26',
            'post_acq_timestamp': '2024-02-26 10:04:29+00:00',
            'track_number': 156,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-02-28',
            'post_acq_timestamp': '2024-02-28 23:28:36+00:00',
            'track_number': 18,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-03-04',
            'post_acq_timestamp': '2024-03-04 23:36:46+00:00',
            'track_number': 91,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-03-09',
            'post_acq_timestamp': '2024-03-09 10:04:29+00:00',
            'track_number': 156,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-03-11',
            'post_acq_timestamp': '2024-03-11 23:28:36+00:00',
            'track_number': 18,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-03-16',
            'post_acq_timestamp': '2024-03-16 23:36:46+00:00',
            'track_number': 91,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-03-21',
            'post_acq_timestamp': '2024-03-21 10:04:30+00:00',
            'track_number': 156,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-03-23',
            'post_acq_timestamp': '2024-03-23 23:28:36+00:00',
            'track_number': 18,
        },
        {
            'mgrs_tile_id': '19HBD',
            'post_acq_date': '2024-03-28',
            'post_acq_timestamp': '2024-03-28 23:36:46+00:00',
            'track_number': 91,
        },
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
        delta_window_days=60,
        max_pre_imgs_per_burst=5,
        df_ts=df_ts,
    )

    assert workflow_inputs == expected_output
