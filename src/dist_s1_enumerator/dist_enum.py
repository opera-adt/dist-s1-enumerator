from datetime import datetime, timedelta

import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from dist_s1_enumerator.asf import get_rtc_s1_temporal_group_metadata
from dist_s1_enumerator.mgrs_burst_data import get_lut_by_mgrs_tile_ids


def enumerate_one_dist_s1_product(mgrs_tile_id: str,
                                  track_number: int,
                                  post_date: datetime,
                                  post_date_buffer_days: int = 1,
                                  max_pre_imgs_per_burst: int = 10,
                                  delta_window_days: int = 365,
                                  delta_lookback_days: int = 0) -> gpd.GeoDataFrame:
    """Enumerate a single product using unique DIST-S1 identifiers.

    The key identifiers are:

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
    post_date : datetime
        Approximate date of post-image Acquistion
    post_date_buffer_days : int, optional
        Number of days around the specified post date to search for post-image
        RTC-S1 data
    max_pre_imgs_per_burst : int, optional
        Number of pre-images per bust to include, by default 10
    delta_window_days : int, optional
        The acceptable window of time to search for pre-image RTC-S1 data
        (latest date is determine delta_lookback_days), by default 365
    delta_lookback_days : int, optional
        The latest acceptable date to search for pre-image RTC-S1 data, by
        default 0, i.e. an immediate lookback

    Returns
    -------
    gpd.GeoDataFrame
    """
    # TODO: Check if we can specify a huge range and still get expected result
    if post_date_buffer_days >= 6:
        raise ValueError('post_date_buffer_days must be less than 6 (S1 pass length) - please check available data')

    df_rtc_post = get_rtc_s1_temporal_group_metadata([mgrs_tile_id],
                                                     track_numbers=[track_number],
                                                     start_acq_dt=post_date + timedelta(days=post_date_buffer_days),
                                                     stop_acq_dt=post_date - timedelta(days=post_date_buffer_days),
                                                     # Should take less than 5 minutes for S1 to pass over MGRS tile
                                                     max_variation_seconds=300,
                                                     n_images_per_burst=1)

    post_date_min = df_rtc_post.acq_dt.min()
    lookback_final = delta_window_days + delta_lookback_days
    df_rtc_pre = get_rtc_s1_temporal_group_metadata([mgrs_tile_id],
                                                    track_numbers=[track_number],
                                                    start_acq_dt=(post_date_min - timedelta(days=lookback_final)),
                                                    stop_acq_dt=(post_date_min - timedelta(days=delta_lookback_days)),
                                                    n_images_per_burst=max_pre_imgs_per_burst)

    df_rtc_pre['input_category'] = 'pre'
    df_rtc_post['input_category'] = 'post'

    df_rtc_product = pd.concat([df_rtc_pre, df_rtc_post], axis=0).reset_index(drop=True)
    return df_rtc_product


def enumerate_dist_s1_products(
    df_rtc_ts: gpd.GeoDataFrame,
    mgrs_tile_ids: list[str],
    n_pre_images_per_burst_target: int = 10,
    min_pre_images_per_burst: int = 2,
    tqdm_enable: bool = True,
    lookback_days_max: int = 365,
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
    df_lut_all = get_lut_by_mgrs_tile_ids(mgrs_tile_ids)

    products = []
    product_id = 0
    for mgrs_tile_id in tqdm(mgrs_tile_ids, desc='Enumerating by MGRS tiles', disable=(not tqdm_enable)):
        df_lut_tile = df_lut_all[df_lut_all.mgrs_tile_id == mgrs_tile_id].reset_index(drop=True)
        acq_group_ids = df_lut_tile.acq_group_id_within_mgrs_tile.unique().tolist()
        # Groups are analogs to tracks (excepted grouped around the equator to ensure a single pass is grouped properly)
        for group_id in acq_group_ids:
            df_burst_ids_pass = (
                df_lut_tile[df_lut_tile.acq_group_id_within_mgrs_tile == group_id].jpl_burst_id.unique().tolist()
            )
            df_rtc_ts_tile_track = df_rtc_ts[df_rtc_ts.jpl_burst_id.isin(df_burst_ids_pass)].reset_index(drop=True)
            # Pass ids are just the cycle with respect to the s1 pass
            min_date = df_rtc_ts_tile_track.acq_dt.min()
            pass_ids = df_rtc_ts_tile_track.acq_dt.map(lambda dt: (dt - min_date).total_seconds() / 86400 / 6).astype(
                int
            )
            df_rtc_ts_tile_track['pass_id'] = pass_ids
            # Latest pass is now the first to appear in the list of pass_ids
            pass_ids_unique = sorted(df_rtc_ts_tile_track.pass_id.unique().tolist(), reverse=True)
            # Now traverse over all the passes
            for pass_id in pass_ids_unique:
                # post
                df_rtc_post = df_rtc_ts_tile_track[df_rtc_ts_tile_track.pass_id == pass_id].reset_index(drop=True)
                df_rtc_post['input_category'] = 'post'

                # pre
                post_date = df_rtc_post.acq_dt.min()
                ind = (df_rtc_ts_tile_track.acq_dt < post_date) & (
                    df_rtc_ts_tile_track.acq_dt >= (post_date - pd.Timedelta(lookback_days_max, unit='D'))
                )
                df_rtc_pre = df_rtc_ts_tile_track[ind].reset_index(drop=True)
                df_rtc_pre['input_category'] = 'pre'

                # This should already be true, but done to for clarity
                df_rtc_pre = df_rtc_pre.sort_values(by='acq_dt', ascending=True).reset_index(drop=True)
                # Assume the data is sorted by acquisition date
                df_rtc_pre = (
                    df_rtc_pre.groupby('jpl_burst_id').tail(n_pre_images_per_burst_target).reset_index(drop=True)
                )

                # product and provenance
                df_rtc_product = pd.concat([df_rtc_pre, df_rtc_post]).reset_index(drop=True)
                df_rtc_product['product_id'] = product_id
                df_rtc_product['mgrs_tile_id'] = mgrs_tile_id
                df_rtc_product['acq_group_id_within_mgrs_tile'] = group_id
                df_rtc_product['pass_id'] = pass_id

                # Remove bursts that are not in both pre and post images
                pre_post_vals = df_rtc_product.groupby('jpl_burst_id').input_category.nunique()
                df_rtc_product = df_rtc_product[
                    df_rtc_product.jpl_burst_id.isin(pre_post_vals[pre_post_vals == 2].index)
                ].reset_index(drop=True)

                # Remove bursts that don't have minimum number of pre images
                pre_counts = df_rtc_product[df_rtc_product.input_category == 'pre'].groupby('jpl_burst_id').size()
                burst_ids_with_min_pre_images = pre_counts[pre_counts >= min_pre_images_per_burst].index.tolist()
                df_rtc_product = df_rtc_product[
                    df_rtc_product.jpl_burst_id.isin(burst_ids_with_min_pre_images)
                ].reset_index(drop=True)

                # findalize products
                if not df_rtc_product.empty:
                    products.append(df_rtc_product)
                    product_id += 1

    df_prods = pd.concat(products, axis=0).reset_index(drop=True)
    return df_prods
