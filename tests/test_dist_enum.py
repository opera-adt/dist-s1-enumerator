import random
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pandera.pandas import check_input
from pytest_mock import MockerFixture

from dist_s1_enumerator.dist_enum import enumerate_dist_s1_products, enumerate_one_dist_s1_product
from dist_s1_enumerator.param_models import LookbackStrategyParams
from dist_s1_enumerator.tabular_models import rtc_s1_resp_schema, rtc_s1_schema


def read_rtc_s1_ts(
    mgrs_tile_ids: list[str] | str,
    track_numbers: list[int] | None = None,
) -> gpd.GeoDataFrame:
    if isinstance(mgrs_tile_ids, str):
        raise TypeError('mgrs_tile_ids must be a list')
    mgrs_tile_token = '_'.join(mgrs_tile_ids)

    file_name = f'mgrs{mgrs_tile_token}.parquet'
    if track_numbers is not None:
        track_token = '_'.join(list(map(str, track_numbers)))
        file_name = file_name.replace('.parquet', f'__track{track_token}.parquet')
    data_dir = Path(__file__).parent / 'data' / 'rtc_s1_ts_metadata'
    parquet_path = data_dir / file_name

    df_rtc_ts = gpd.read_parquet(parquet_path)
    df_rtc_ts = df_rtc_ts.drop_duplicates(subset=['opera_id']).reset_index(drop=True)

    return df_rtc_ts


@check_input(rtc_s1_schema, 0)
def mock_response_from_asf_daac(
    df_rtc_ts: gpd.GeoDataFrame,
    start_acq_dt: pd.Timestamp,
    stop_acq_dt: pd.Timestamp,
    track_numbers: list[int],
    mgrs_tile_id: str,
) -> gpd.GeoDataFrame:
    df_resp = df_rtc_ts.copy()
    ind = (df_resp['acq_dt'] >= start_acq_dt) & (df_resp['acq_dt'] <= stop_acq_dt)
    ind = ind & (df_resp['track_number'].isin(track_numbers))
    ind = ind & (df_resp['mgrs_tile_id'] == mgrs_tile_id)
    df_resp = df_resp[ind].reset_index(drop=True)
    df_resp = df_resp[rtc_s1_resp_schema.columns.keys()]
    return df_resp


@pytest.mark.parametrize(
    'mgrs_tile_ids,track_numbers,lookback_strategy,delta_lookback_days,delta_window_days,'
    'max_pre_imgs_per_burst,min_pre_imgs_per_burst',
    [
        (['15RXN'], [63], 'immediate_lookback', 0, 365, 10, 2),  # Waxlake delta, VV+VH
        (['22WFD'], None, 'immediate_lookback', 0, 365, 10, 2),  # greenland, all tracks, and HH+HV
        (
            ['11SLT', '11SLU', '11SMT'],
            None,
            'immediate_lookback',
            0,
            365,
            10,
            2,
        ),  # multiple MGRS tiles over Los Angeles
        (['01UBT'], None, 'immediate_lookback', 0, 365, 10, 2),  # Aleutian Chain at the antimeridian
        (['15RXN'], [63], 'multi_window', 365, 365, (5, 5, 5), 1),  # Waxlake delta, VV+VH
    ],
)
def test_dist_enum_default_strategies(
    lookback_strategy: str,
    delta_lookback_days: int | list[int] | tuple[int, ...],
    delta_window_days: int | list[int] | tuple[int, ...],
    max_pre_imgs_per_burst: int | list[int] | tuple[int, ...],
    min_pre_imgs_per_burst: int | list[int] | tuple[int, ...],
    mgrs_tile_ids: list[str],
    track_numbers: list[int] | None,
    mocker: MockerFixture,
) -> None:
    if not isinstance(mgrs_tile_ids, list):
        raise TypeError('mgrs_tile_ids must be a list')

    df_rtc_s1_ts = read_rtc_s1_ts(mgrs_tile_ids, track_numbers=track_numbers)

    params = LookbackStrategyParams(
        lookback_strategy=lookback_strategy,
        delta_lookback_days=delta_lookback_days,
        delta_window_days=delta_window_days,
        max_pre_imgs_per_burst=max_pre_imgs_per_burst,
        min_pre_imgs_per_burst=min_pre_imgs_per_burst,
    )
    df_products = enumerate_dist_s1_products(
        df_rtc_s1_ts,
        mgrs_tile_ids,
        lookback_strategy=lookback_strategy,
        delta_lookback_days=delta_lookback_days,
        delta_window_days=delta_window_days,
        max_pre_imgs_per_burst=max_pre_imgs_per_burst,
        min_pre_imgs_per_burst=min_pre_imgs_per_burst,
    )

    # Get unique product_ids and their corresponding acq_date_for_mgrs_pass
    df_post = df_products[df_products.input_category == 'post'].reset_index(drop=True)
    df_tmp = (
        df_post[['product_id', 'acq_date_for_mgrs_pass', 'track_number', 'mgrs_tile_id', 'track_token']]
        .drop_duplicates(subset='product_id')
        .sort_values(by='acq_date_for_mgrs_pass')
    )
    mgrs_tile_ids_post = df_tmp['mgrs_tile_id'].tolist()
    product_ids = df_tmp['product_id'].tolist()
    post_dates = df_tmp['acq_date_for_mgrs_pass'].tolist()
    track_tokens_post = df_tmp['track_token'].tolist()
    track_numbers_post_lst = [[int(track) for track in token.split('_')] for token in track_tokens_post]

    assert len(mgrs_tile_ids_post) == len(product_ids) == len(post_dates) == len(track_numbers_post_lst)

    PRODS_TO_TEST = 25  # can set to len(product_ids) to run all products
    indices = random.sample(range(len(product_ids)), PRODS_TO_TEST)
    product_ids = [product_ids[i] for i in indices]
    mgrs_tile_ids_post = [mgrs_tile_ids_post[i] for i in indices]
    post_dates = [post_dates[i] for i in indices]
    track_numbers_post_lst = [track_numbers_post_lst[i] for i in indices]

    dfs_post = [
        mock_response_from_asf_daac(
            df_rtc_s1_ts,
            pd.Timestamp(post_date, tz='UTC') - pd.Timedelta(1, unit='D'),
            pd.Timestamp(post_date, tz='UTC') + pd.Timedelta(1, unit='D'),
            track_numbers_post,
            mgrs_tile_id,
        )
        for post_date, track_numbers_post, mgrs_tile_id in zip(post_dates, track_numbers_post_lst, mgrs_tile_ids_post)
    ]
    if lookback_strategy == 'immediate_lookback':
        dfs_pre = [
            mock_response_from_asf_daac(
                df_rtc_s1_ts,
                pd.Timestamp(post_date, tz='UTC') - pd.Timedelta(delta_window_days + delta_lookback_days + 1, unit='D'),
                pd.Timestamp(post_date, tz='UTC') - pd.Timedelta(delta_lookback_days + 1, unit='D'),
                track_numbers_post,
                mgrs_tile_id,
            )
            for post_date, track_numbers_post, mgrs_tile_id in zip(
                post_dates, track_numbers_post_lst, mgrs_tile_ids_post
            )
        ]
        side_effects = [df for group in zip(dfs_post, dfs_pre) for df in group]
    elif lookback_strategy == 'multi_window':
        dfs_pre = [
            mock_response_from_asf_daac(
                df_rtc_s1_ts,
                pd.Timestamp(post_date, tz='UTC')
                - pd.Timedelta(delta_window_days + delta_lookback_day_item + 1, unit='D'),
                pd.Timestamp(post_date, tz='UTC') - pd.Timedelta(delta_lookback_day_item + 1, unit='D'),
                track_numbers_post,
                mgrs_tile_id,
            )
            for post_date, track_numbers_post, mgrs_tile_id in zip(
                post_dates, track_numbers_post_lst, mgrs_tile_ids_post
            )
            for delta_lookback_day_item in params.delta_lookback_days
        ]
        side_effects = []
        for k in range(len(dfs_post)):
            N_lookbacks = len(params.delta_lookback_days)
            side_effects.append(dfs_post[k])
            start_pre_idx = k * N_lookbacks
            end_pre_idx = (k + 1) * N_lookbacks
            side_effects.extend(dfs_pre[start_pre_idx:end_pre_idx])

    mocker.patch('dist_s1_enumerator.asf.get_rtc_s1_ts_metadata_by_burst_ids', side_effect=side_effects)

    for product_id, mgrs_tile_id, post_date, track_numbers_post in zip(
        product_ids,
        mgrs_tile_ids_post,
        post_dates,
        track_numbers_post_lst,
    ):
        print(product_id)
        df_one_product = enumerate_one_dist_s1_product(
            mgrs_tile_id,
            track_numbers_post,
            pd.Timestamp(post_date),
            lookback_strategy=lookback_strategy,
            delta_lookback_days=delta_lookback_days,
            delta_window_days=delta_lookback_days,
            max_pre_imgs_per_burst=max_pre_imgs_per_burst,
            min_pre_imgs_per_burst=min_pre_imgs_per_burst,
        )
        df_one_product_alt = (
            df_products[df_products.product_id == product_id].reset_index(drop=True).drop(columns='product_id')
        )
        df_pre_alt = (
            df_one_product_alt[df_one_product_alt.input_category == 'pre']
            .sort_values(by='opera_id')
            .reset_index(drop=True)
        )
        df_post_alt = (
            df_one_product_alt[df_one_product_alt.input_category == 'post']
            .sort_values(by='opera_id')
            .reset_index(drop=True)
        )

        df_pre = (
            df_one_product[df_one_product.input_category == 'pre'].sort_values(by='opera_id').reset_index(drop=True)
        )
        df_post = (
            df_one_product[df_one_product.input_category == 'post'].sort_values(by='opera_id').reset_index(drop=True)
        )

        assert_frame_equal(df_pre, df_pre_alt, atol=1e-7)
        assert_frame_equal(df_post, df_post_alt, atol=1e-7)


@pytest.mark.parametrize(
    'mgrs_tile_ids, track_numbers',
    [
        (['15RXN'], [63]),  # Waxlake delta, VV+VH
    ],
)
def test_burst_ids_consistent_between_pre_and_post(mgrs_tile_ids: list[str], track_numbers: list[int] | None) -> None:
    if not isinstance(mgrs_tile_ids, list):
        raise TypeError('mgrs_tile_ids must be a list')

    delta_window_days = 365
    delta_lookback_days = 0
    max_pre_imgs_per_burst = 10
    min_pre_imgs_per_burst = 2
    df_rtc_s1_ts = read_rtc_s1_ts(mgrs_tile_ids, track_numbers=track_numbers)

    df_products = enumerate_dist_s1_products(
        df_rtc_s1_ts,
        mgrs_tile_ids,
        lookback_strategy='immediate_lookback',
        delta_lookback_days=delta_lookback_days,
        delta_window_days=delta_window_days,
        max_pre_imgs_per_burst=max_pre_imgs_per_burst,
        min_pre_imgs_per_burst=min_pre_imgs_per_burst,
    )

    # Check the burst ids are consistent between pre and post
    for product_id in df_products['product_id'].unique():
        df_product = df_products[df_products['product_id'] == product_id].reset_index(drop=True)
        df_pre = df_product[df_product['input_category'] == 'pre'].reset_index(drop=True)
        df_post = df_product[df_product['input_category'] == 'post'].reset_index(drop=True)
        assert sorted(df_pre['jpl_burst_id'].unique().tolist()) == sorted(df_post['jpl_burst_id'].unique().tolist())
