from datetime import datetime, timedelta

import geopandas as gpd
import pandas as pd
from pandera.pandas import check_input
from tqdm.auto import tqdm

from dist_s1_enumerator.asf import get_rtc_s1_metadata_from_acq_group
from dist_s1_enumerator.data_models import dist_s1_input_schema, reorder_columns, rtc_s1_schema


def enumerate_one_dist_s1_product(
    mgrs_tile_id: str,
    track_number: int | list[int],
    post_date: datetime | pd.Timestamp | str,
    lookback_strategy: str = 'immediate_lookback',
    post_date_buffer_days: int = 1,
    max_pre_imgs_per_burst: int = 10,
    max_pre_imgs_per_burst_mw: list[int] = [5, 5],
    delta_window_days: int = 365,
    delta_lookback_days: int = 0,
    delta_lookback_days_mw: list[int] = [365 * 2, 365 * 1],
    min_pre_imgs_per_burst: int = 2,
) -> gpd.GeoDataFrame:
    """Enumerate a single product using unique DIST-S1 identifiers.

    The product identifiers are:

    1. MGRS Tile
    2. Track Number
    3. Post-image date (with a buffer)

    Hits the ASF DAAC API to get the necessary pre-/post-image data. Not
    recommended for enumerating large numbers of products over multiple MGRS
    tiles and/or track numbers.

    Parameters
    ----------
    mgrs_tile_id : str
        MGRS tile for DIST-S1 product
    track_number : int
        Track number for RTC-S1 pass
    post_date : datetime | pd.Timestamp | str
        Approximate date of post-image Acquistion, if string should be in the form of 'YYYY-MM-DD'.
    post_date_buffer_days : int, optional
        Number of days around the specified post date to search for post-image
        RTC-S1 data
    lookback_strategy : str, optional
        Lookback strategy to use, by default 'multi_window'. Options are
        'immediate_lookback' or 'multi_window'. The immediate lookback
    max_pre_imgs_per_burst : int, optional
        Number of pre-images per bust to include, by default 10
    max_pre_imgs_per_burst_mw : list[int], optional
        Number of pre-images per burst to include for each lookback window
        (multi-window strategy), by default [5, 5]
    delta_window_days : int, optional
        The acceptable window of time to search for pre-image RTC-S1 data
        (latest date is determine delta_lookback_days), by default 365
    delta_lookback_days : int, optional
        The latest acceptable date to search for pre-image RTC-S1 data, by
        default 0, i.e. an immediate lookback
    delta_lookback_days_mw : list[int], optional
        The latest acceptable date to search for pre-image RTC-S1 data for
        each lookback window (multi-window strategy), by default [365*2, 365*1]
    min_pre_imgs_per_burst : int, optional
        Minimum number of pre-images per burst to include, by default 2

    Returns
    -------
    gpd.GeoDataFrame
    """
    # TODO: Check if we can specify a huge range and still get expected result
    if isinstance(post_date, str):
        post_date = pd.Timestamp(post_date)

    if post_date_buffer_days >= 6:
        raise ValueError('post_date_buffer_days must be less than 6 (S1 pass length) - please check available data')

    if isinstance(track_number, int):
        track_numbers = [track_number]
    elif isinstance(track_number, list):
        track_numbers = track_number
    else:
        raise TypeError('track_number must be a single integer or a list of integers.')

    if isinstance(mgrs_tile_id, list):
        raise TypeError('mgrs_tile_id must be a single string; we are enumerating inputs for a single DIST-S1 product.')

    if isinstance(post_date, pd.Timestamp):
        post_date = post_date.to_pydatetime()

    df_rtc_post = get_rtc_s1_metadata_from_acq_group(
        [mgrs_tile_id],
        track_numbers=track_numbers,
        start_acq_dt=post_date + timedelta(days=post_date_buffer_days),
        stop_acq_dt=post_date - timedelta(days=post_date_buffer_days),
        # Should take less than 5 minutes for S1 to pass over MGRS tile
        max_variation_seconds=300,
        n_images_per_burst=1,
    )

    if df_rtc_post.empty:
        raise ValueError(f'No RTC-S1 post-images found for track {track_number} in MGRS tile {mgrs_tile_id}.')

    if lookback_strategy == 'immediate_lookback':
        print('Using immediate lookback strategy')
        # Add 5 minutes buffer to ensure we don't include post-images in pre-image set.
        post_date_min = df_rtc_post.acq_dt.min() - pd.Timedelta(seconds=300)
        lookback_final = delta_window_days + delta_lookback_days
        start_acq_dt = post_date_min - timedelta(days=lookback_final)
        stop_acq_dt = post_date_min - timedelta(days=delta_lookback_days)
        df_rtc_pre = get_rtc_s1_metadata_from_acq_group(
            [mgrs_tile_id],
            track_numbers=track_numbers,
            start_acq_dt=start_acq_dt,
            stop_acq_dt=stop_acq_dt,
            n_images_per_burst=max_pre_imgs_per_burst,
        )

    elif lookback_strategy == 'multi_window':
        print('Using multi-window lookback strategy')
        df_rtc_pre_list = []
        for delta_lookback_day, max_pre_img_per_burst in zip(delta_lookback_days_mw, max_pre_imgs_per_burst_mw):
            # Add 5 minutes buffer to ensure we don't include post-images in pre-image set.
            post_date_min = df_rtc_post.acq_dt.min() - pd.Timedelta(seconds=300)
            lookback_final = delta_window_days + delta_lookback_day
            start_acq_dt = post_date_min - timedelta(days=lookback_final)
            stop_acq_dt = post_date_min - timedelta(days=delta_lookback_day)
            df_rtc_pre = get_rtc_s1_metadata_from_acq_group(
                [mgrs_tile_id],
                track_numbers=track_numbers,
                start_acq_dt=start_acq_dt,
                stop_acq_dt=stop_acq_dt,
                n_images_per_burst=max_pre_img_per_burst,
            )

            if not df_rtc_pre.empty:
                df_rtc_pre_list.append(df_rtc_pre)

        df_rtc_pre = pd.concat(df_rtc_pre_list, ignore_index=True) if df_rtc_pre_list else pd.DataFrame()

    else:
        raise ValueError(
            f'Unsupported lookback_strategy: {lookback_strategy}. Expected "multi_window" or "immediate_lookback".'
        )

    pre_counts = df_rtc_pre.groupby('jpl_burst_id').size()
    burst_ids_with_min_pre_images = pre_counts[pre_counts >= min_pre_imgs_per_burst].index.tolist()
    df_rtc_pre = df_rtc_pre[df_rtc_pre.jpl_burst_id.isin(burst_ids_with_min_pre_images)].reset_index(drop=True)

    post_burst_ids = df_rtc_post.jpl_burst_id.unique().tolist()
    pre_burst_ids = df_rtc_post.jpl_burst_id.unique().tolist()

    final_burst_ids = list(set(post_burst_ids) & set(pre_burst_ids))
    df_rtc_pre = df_rtc_pre[df_rtc_pre.jpl_burst_id.isin(final_burst_ids)].reset_index(drop=True)
    df_rtc_post = df_rtc_post[df_rtc_post.jpl_burst_id.isin(final_burst_ids)].reset_index(drop=True)

    if df_rtc_pre.empty:
        raise ValueError(
            f'Not enough RTC-S1 pre-images found for track {track_number} in MGRS tile {mgrs_tile_id} '
            'with available pre-images.'
        )
    if df_rtc_post.empty:
        raise ValueError(
            f'Not enough RTC-S1 post-images found for track {track_number} in MGRS tile {mgrs_tile_id} '
            'with available pre-images.'
        )

    df_rtc_pre['input_category'] = 'pre'
    df_rtc_post['input_category'] = 'post'

    df_rtc_product = pd.concat([df_rtc_pre, df_rtc_post], axis=0).reset_index(drop=True)

    # Validation
    dist_s1_input_schema.validate(df_rtc_product)
    df_rtc_product = reorder_columns(df_rtc_product, dist_s1_input_schema)

    return df_rtc_product


@check_input(rtc_s1_schema, 0)
def enumerate_dist_s1_products(
    df_rtc_ts: gpd.GeoDataFrame,
    mgrs_tile_ids: list[str],
    lookback_strategy: str = 'immediate_lookback',
    max_pre_imgs_per_burst: int = 10,
    max_pre_imgs_per_burst_mw: list[int] = [5, 5],
    min_pre_imgs_per_burst: int = 2,
    tqdm_enabled: bool = True,
    delta_lookback_days: int = 0,
    delta_window_days: int = 365,
    delta_lookback_days_mw: list[int] = [365 * 2, 365 * 1],
) -> gpd.GeoDataFrame:
    """
    Enumerate from a stack of RTC-S1 metadata and MGRS tile.

    Ensures we do not have to continually hit the ASF DAAC API.

    Parameters
    ----------
    df_rtc_ts : gpd.GeoDataFrame
        RTC-S1 data
    mgrs_tile_ids : list[str]
        List of MGRS tiles to enumerate
    n_pre_images_per_burst_target : int, optional
        Number of pre-images per burst to include, by default 10
    min_pre_images_per_burst : int, optional
        Minimum number of pre-images per burst to include, by default 2
    tqdm_enable : bool, optional
        Whether to enable tqdm progress bars, by default True
    lookback_days_max : int, optional
        Maximum number of days to search for pre-image RTC-S1 data, by default
        365
    """
    if lookback_strategy == 'immediate_lookback':
        print('Using immediate lookback strategy')
        if max_pre_imgs_per_burst < min_pre_imgs_per_burst:
            raise ValueError('max_pre_imgs_per_burst must be greater than min_pre_imgs_per_burst')
    if lookback_strategy == 'multi_window':
        print('Using multi-window lookback strategy')
        if any(m < min_pre_imgs_per_burst for m in max_pre_imgs_per_burst_mw):
            raise ValueError('All values in max_pre_imgs_per_burst must be greater than min_pre_imgs_per_burst')

    products = []
    product_id = 0
    for mgrs_tile_id in tqdm(mgrs_tile_ids, desc='Enumerate by MGRS tiles', disable=(not tqdm_enabled)):
        df_rtc_ts_tile = df_rtc_ts[df_rtc_ts.mgrs_tile_id == mgrs_tile_id].reset_index(drop=True)
        acq_group_ids_in_tile = df_rtc_ts_tile.acq_group_id_within_mgrs_tile.unique().tolist()
        # Groups are analogs to tracks (excepted grouped around the equator to ensure a single pass is grouped properly)
        for group_id in acq_group_ids_in_tile:
            df_rtc_ts_tile_track = df_rtc_ts_tile[df_rtc_ts_tile.acq_group_id_within_mgrs_tile == group_id].reset_index(
                drop=True
            )
            # Latest pass is now the first to appear in the list of pass_ids
            pass_ids_unique = sorted(df_rtc_ts_tile_track.pass_id.unique().tolist(), reverse=True)
            # Now traverse over all the passes
            for pass_id in pass_ids_unique:
                # post
                df_rtc_post = df_rtc_ts_tile_track[df_rtc_ts_tile_track.pass_id == pass_id].reset_index(drop=True)
                df_rtc_post['input_category'] = 'post'

                if lookback_strategy == 'immediate_lookback':
                    # pre-image accounting
                    post_date = df_rtc_post.acq_dt.min()
                    delta_lookback = pd.Timedelta(delta_lookback_days, unit='D')
                    delta_window = pd.Timedelta(delta_window_days, unit='D')
                    window_start = post_date - delta_lookback - delta_window
                    window_stop = post_date - delta_lookback

                    # pre-image filtering
                    # Select pre-images temporally
                    ind_time = (df_rtc_ts_tile_track.acq_dt < window_stop) & (
                        df_rtc_ts_tile_track.acq_dt >= window_start
                    )
                    # Select images that are present in the post-image
                    ind_burst = df_rtc_ts_tile_track.jpl_burst_id.isin(df_rtc_post.jpl_burst_id)
                    ind = ind_time & ind_burst
                    df_rtc_pre = df_rtc_ts_tile_track[ind].reset_index(drop=True)
                    df_rtc_pre['input_category'] = 'pre'

                    # It is unclear how merging when multiple MGRS tiles are provided will impact order so this
                    # is done to ensure the most recent pre-image set for each burst is selected
                    df_rtc_pre = df_rtc_pre.sort_values(by='acq_dt', ascending=True).reset_index(drop=True)
                    # Assume the data is sorted by acquisition date
                    df_rtc_pre = df_rtc_pre.groupby('jpl_burst_id').tail(max_pre_imgs_per_burst).reset_index(drop=True)
                    if df_rtc_pre.empty:
                        continue

                    # product and provenance
                    df_rtc_product = pd.concat([df_rtc_pre, df_rtc_post]).reset_index(drop=True)
                    df_rtc_product['product_id'] = product_id

                elif lookback_strategy == 'multi_window':
                    # pre-image accounting
                    post_date = df_rtc_post.acq_dt.min()
                    # Loop over the different lookback days
                    df_rtc_pre_list = []
                    for delta_lookback_day, max_pre_img_per_burst in zip(
                        delta_lookback_days_mw, max_pre_imgs_per_burst_mw
                    ):
                        delta_lookback = pd.Timedelta(delta_lookback_day, unit='D')
                        delta_window = pd.Timedelta(delta_window_days, unit='D')
                        window_start = post_date - delta_lookback - delta_window
                        window_stop = post_date - delta_lookback

                        # pre-image filtering
                        # Select pre-images temporally
                        ind_time = (df_rtc_ts_tile_track.acq_dt < window_stop) & (
                            df_rtc_ts_tile_track.acq_dt >= window_start
                        )
                        # Select images that are present in the post-image
                        ind_burst = df_rtc_ts_tile_track.jpl_burst_id.isin(df_rtc_post.jpl_burst_id)
                        ind = ind_time & ind_burst
                        df_rtc_pre = df_rtc_ts_tile_track[ind].reset_index(drop=True)
                        df_rtc_pre['input_category'] = 'pre'

                        # It is unclear how merging when multiple MGRS tiles are provided will impact order so this
                        # is done to ensure the most recent pre-image set for each burst is selected
                        df_rtc_pre = df_rtc_pre.sort_values(by='acq_dt', ascending=True).reset_index(drop=True)
                        # Assume the data is sorted by acquisition date
                        df_rtc_pre = (
                            df_rtc_pre.groupby('jpl_burst_id').tail(max_pre_img_per_burst).reset_index(drop=True)
                        )

                        if df_rtc_pre.empty:
                            continue

                        if not df_rtc_pre.empty:
                            df_rtc_pre_list.append(df_rtc_pre)  # Store each df_rtc_pre

                    # Concatenate all df_rtc_pre into a single DataFrame
                    df_rtc_pre_final = (
                        pd.concat(df_rtc_pre_list, ignore_index=True) if df_rtc_pre_list else pd.DataFrame()
                    )
                    # df_rtc_pre_final = df_rtc_pre_final.sort_values(
                    #     by='acq_dt', ascending=True
                    # ).reset_index(drop=True)  # Sort by acq_dt

                    # product and provenance
                    df_rtc_product = pd.concat([df_rtc_pre_final, df_rtc_post]).reset_index(drop=True)
                    df_rtc_product['product_id'] = product_id

                else:
                    raise ValueError(
                        f'Unsupported lookback_strategy: {lookback_strategy}. '
                        'Expected "multi_window" or "immediate_lookback".'
                    )

                # Remove bursts that don't have minimum number of pre images
                pre_counts = df_rtc_product[df_rtc_product.input_category == 'pre'].groupby('jpl_burst_id').size()
                burst_ids_with_min_pre_images = pre_counts[pre_counts >= min_pre_imgs_per_burst].index.tolist()
                df_rtc_product = df_rtc_product[
                    df_rtc_product.jpl_burst_id.isin(burst_ids_with_min_pre_images)
                ].reset_index(drop=True)

                # finalize products
                if not df_rtc_product.empty:
                    products.append(df_rtc_product)
                    product_id += 1
    df_prods = pd.concat(products, axis=0).reset_index(drop=True)

    dist_s1_input_schema.validate(df_prods)
    df_prods = reorder_columns(df_prods, dist_s1_input_schema)

    return df_prods
