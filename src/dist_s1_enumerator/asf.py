from datetime import datetime
from warnings import warn

import asf_search as asf
import geopandas as gpd
import pandas as pd
from rasterio.crs import CRS
from shapely.geometry import shape


def get_asf_rtc_s1_metadata_for_fixed_track(
    burst_ids: str | list[str],
    earliest_acceptable_acq_dt: str | datetime = None,
    latest_acceptable_acq_dt: str | datetime = None,
) -> gpd.GeoDataFrame:
    """
    Get ASF RTC burst metadata for a fixed track. The track number is extracted from the burst_ids.
    """
    if isinstance(burst_ids, str):
        burst_ids = [burst_ids]
    # make sure JPL syntax is transformed to asf syntax
    burst_ids = [burst_id.upper().replace('-', '_') for burst_id in burst_ids]

    # Ensure there is at most one track number or 2 sequential tracks
    unique_tracks = list(set([int(b_id.split('_')[0][1:]) for b_id in burst_ids]))
    unique_tracks_str = ', '.join(map(str, unique_tracks))
    if len(unique_tracks) > 2:
        raise ValueError(f'More than 2 unique track numbers found: {unique_tracks_str}')
    if len(unique_tracks) == 2 and unique_tracks[0] + 1 != unique_tracks[1]:
        raise ValueError(f'Non-sequential track numbers found for tracks {unique_tracks_str}.')

    resp = asf.search(
        operaBurstID=burst_ids,
        processingLevel='RTC',
        polarization=['VV', 'VH'],
        start=earliest_acceptable_acq_dt,
        end=latest_acceptable_acq_dt,
    )
    if not resp:
        raise warn('No results - please check burst id and availability.', category=UserWarning)
        return gpd.GeoDataFrame()

    properties = [r.properties for r in resp]
    geometry = [shape(r.geojson()['geometry']) for r in resp]
    properties_f = [
        {
            'opera_id': p['sceneName'],
            'acq_dt': pd.to_datetime(p['startTime']),
            'polarization': '+'.join(p['polarization']),
            'url_vh': p['url'],
            'url_vv': (p['url'].replace('_VH.tif', '_VV.tif')),
            'track_number': p['pathNumber'],
        }
        for p in properties
    ]

    df_rtc = gpd.GeoDataFrame(properties_f, geometry=geometry, crs=CRS.from_epsg(4326))
    # Ensure dual polarization
    df_rtc['jpl_burst_id'] = df_rtc['opera_id'].map(lambda bid: bid.split('_')[3])
    df_rtc = df_rtc[df_rtc.polarization == 'VV+VH'].reset_index(drop=True)
    df_rtc = df_rtc.sort_values(by=['jpl_burst_id', 'acq_dt']).reset_index(drop=True)

    # Remove duplicates from time series
    df_rtc['dedup_id'] = df_rtc.opera_id.map(lambda id_: '_'.join(id_.split('_')[:5]))
    df_rtc = df_rtc.drop_duplicates(subset=['dedup_id']).reset_index(drop=True)
    df_rtc = df_rtc.drop(columns=['dedup_id'])

    # Group by acquisition time to ensure that the acquisition date is grouped by date of earliest time in pass
    # We deal with midnight crossing by shifting the time by 10 minutes
    midnight_crossing = (df_rtc['acq_dt'].dt.hour == 0).any() & (df_rtc['acq_dt'].dt.hour == 23).any()
    time_offset = pd.Timedelta('10 minutes')
    if midnight_crossing:
        df_rtc['acq_date'] = (df_rtc['acq_dt'] - time_offset * (df_rtc['acq_dt'].dt.hour == 0)).dt.date.astype(str)
    else:
        df_rtc['acq_date'] = df_rtc['acq_dt'].dt.date.astype(str)

    df_rtc = df_rtc[
        [
            'opera_id',
            'jpl_burst_id',
            'acq_dt',
            'acq_date',
            'polarization',
            'url_vh',
            'url_vv',
            'track_number',
            'geometry',
        ]
    ]
    return df_rtc


def get_rtc_s1_ts_metadata(
    burst_ids: list[str],
    earliest_acceptable_acq_dt: datetime,
    latest_acceptable_acq_dt: datetime,
    maximum_variation_in_acq_dts_seconds: float = None,
    n_images_per_burst: int = 1,
) -> gpd.GeoDataFrame:
    """
    Get the most recent burst image for a list of burst ids within a date range.

    For acquiring a post-image set, the intended use is for a group of burst_ids that are acquired in a single S1 pass.
    There is no check that the burst_ids supplied are all from the same S1 acqusition group. As such, we provide the
    keyword argument maximum_variation_in_acq_dts_seconds to ensure the latest acquisition of are within the latest
    acquisition time from the most recent burst image. If this is not provided, you will get the latest burst image
    product for each burst within the allowable date range. This could yield imagery collected on different dates
    for the burst_ids provided.

    For acquiring a pre-image set, we use n_images_per_burst > 1. We get the latest n_images_per_burst images for each
    burst and there can be different number of images per burst for all the burst supplied and/or the image
    time series can be composed of images from different dates.

    Parameters
    ----------
    burst_ids : list[str]
    earliest_acceptable_acq_dt : datetime
    latest_acceptable_acq_dt : datetime
    maximum_variation_in_acq_dts_seconds : float, optional

    Returns
    -------
    gpd.GeoDataFrame
    """
    if n_images_per_burst == 1 and maximum_variation_in_acq_dts_seconds is None:
        warn(
            'No maximum variation in acq dts or n_images_per_burst provided. This could yield imagery collected on '
            'different dates for the burst_ids provided.',
            category=UserWarning,
        )
    df_rtc = get_asf_rtc_s1_metadata_for_fixed_track(
        burst_ids,
        earliest_acceptable_acq_dt=earliest_acceptable_acq_dt,
        latest_acceptable_acq_dt=latest_acceptable_acq_dt,
    )
    columns = df_rtc.columns
    # Assumes that each group is ordered by date (earliest first and most recent last)
    df_rtc = df_rtc.groupby('jpl_burst_id').tail(n_images_per_burst).reset_index(drop=False)
    df_rtc = df_rtc[columns]
    if maximum_variation_in_acq_dts_seconds is not None:
        if n_images_per_burst > 1:
            raise ValueError('Cannot apply maximum variation in acq dts when n_images_per_burst > 1.')
        max_dt = df_rtc['acq_dt'].max()
        ind = df_rtc['acq_dt'] > max_dt - pd.Timedelta(seconds=maximum_variation_in_acq_dts_seconds)
        df_rtc = df_rtc[ind].reset_index(drop=True)
    return df_rtc


def agg_rtc_metadata_by_burst_id(df_rtc: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df_agg = (
        df_rtc.groupby('jpl_burst_id')
        .agg(count=('jpl_burst_id', 'size'), earliest_acq_date=('acq_date', 'min'), latest_acq_date=('acq_date', 'max'))
        .reset_index()
    )

    return df_agg
