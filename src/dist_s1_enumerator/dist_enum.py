from datetime import datetime, timedelta
from warnings import warn

import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from dist_s1_enumerator.mgrs_burst_data import get_lut_by_mgrs_tile_ids


def enumerate_dist_s1_products(
    df_rtc_ts: gpd.GeoDataFrame,
    mgrs_tile_ids: list[str],
    n_pre_images_per_burst_target: int = 10,
    min_pre_images_per_burst: int = 2,
    tqdm_enable: bool = True,
    lookback_days_max: int = 365,
) -> gpd.GeoDataFrame:
    """ """
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
