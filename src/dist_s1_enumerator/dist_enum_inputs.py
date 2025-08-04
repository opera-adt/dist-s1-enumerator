from datetime import datetime

import pandas as pd

from dist_s1_enumerator.asf import get_rtc_s1_ts_metadata_from_mgrs_tiles
from dist_s1_enumerator.dist_enum import enumerate_dist_s1_products


def update_dist_s1_input_dict(data_dict: dict) -> dict:
    out = {}
    out.update(
        {
            key: val
            for (key, val) in data_dict.items()
            if key in ['mgrs_tile_id', 'acq_date_for_mgrs_pass', 'track_number', 'product_id']
        }
    )
    out_formatted = {
        'mgrs_tile_id': out['mgrs_tile_id'],
        'acq_date': out['acq_date_for_mgrs_pass'],
        'track_number': out['track_number'],
        'product_id': out['product_id'],
    }
    return out_formatted


def enumerate_dist_s1_workflow_inputs(
    mgrs_tile_ids: list[str] | str,
    track_numbers: list[int] | int,
    start_acq_dt: datetime | str | None = None,
    stop_acq_dt: datetime | str | None = None,
    lookback_strategy: str = 'multi_window',
) -> list[dict]:
    if isinstance(mgrs_tile_ids, str):
        mgrs_tile_ids = [mgrs_tile_ids]
    if isinstance(track_numbers, int):
        track_numbers = [track_numbers]
    if isinstance(start_acq_dt, str):
        start_acq_dt = pd.to_datetime(start_acq_dt)
    if isinstance(stop_acq_dt, str):
        stop_acq_dt = pd.to_datetime(stop_acq_dt)

    df_ts = get_rtc_s1_ts_metadata_from_mgrs_tiles(
        mgrs_tile_ids,
        track_numbers,
        start_acq_dt,
        stop_acq_dt,
    )
    if start_acq_dt is not None:
        start_ind = df_ts.acq_dt >= pd.Timestamp(start_acq_dt, tz='UTC')
        df_ts = df_ts[start_ind].reset_index(drop=True)
    if stop_acq_dt is not None:
        stop_ind = df_ts.acq_dt <= pd.Timestamp(stop_acq_dt, tz='UTC')
        df_ts = df_ts[stop_ind].reset_index(drop=True)

    df_products = enumerate_dist_s1_products(df_ts, mgrs_tile_ids, lookback_strategy=lookback_strategy)

    df_dist_prod_data = df_products.to_dict('records')

    all_inputs = list(map(update_dist_s1_input_dict, df_dist_prod_data))
    return all_inputs
