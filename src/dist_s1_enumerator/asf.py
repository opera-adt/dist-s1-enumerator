from datetime import datetime
from warnings import warn

import asf_search as asf
import geopandas as gpd
import pandas as pd
from pandera import check_input
from rasterio.crs import CRS
from shapely.geometry import shape

from dist_s1_enumerator.data_models import reorder_columns, rtc_s1_schema
from dist_s1_enumerator.mgrs_burst_data import get_burst_ids_in_mgrs_tiles, get_lut_by_mgrs_tile_ids


def get_rtc_s1_ts_metadata_by_burst_ids(
    burst_ids: str | list[str],
    start_acq_dt: str | datetime = None,
    stop_acq_dt: str | datetime = None,
    polarizations: str = 'VV+VH',
) -> gpd.GeoDataFrame:
    """Get ASF RTC burst metadata for a fixed track. The track number is extracted from the burst_ids."""
    if isinstance(burst_ids, str):
        burst_ids = [burst_ids]

    if polarizations not in ['VV+VH', 'HH+HV']:
        raise ValueError(f'Invalid polarization: {polarizations}. Must be one of: VV+VH, HH+HV.')

    if polarizations == 'VV+VH':
        polarization_list = ['VV', 'VH']
    elif polarizations == 'HH+HV':
        polarization_list = ['HH', 'HV']

    # make sure JPL syntax is transformed to asf syntax
    burst_ids = [burst_id.upper().replace('-', '_') for burst_id in burst_ids]

    resp = asf.search(
        operaBurstID=burst_ids,
        processingLevel='RTC',
        polarization=polarization_list,
        start=start_acq_dt,
        end=stop_acq_dt,
    )
    if not resp:
        raise warn('No results - please check burst id and availability.', category=UserWarning)
        return gpd.GeoDataFrame()

    properties = [r.properties for r in resp]
    geometry = [shape(r.geojson()['geometry']) for r in resp]
    crosspol = polarization_list[0]
    copol = polarization_list[1]
    properties_f = [
        {
            'opera_id': p['sceneName'],
            'acq_dt': pd.to_datetime(p['startTime']),
            'polarizations': polarizations,
            'url_crosspol': p['url'],
            'url_copol': p['url'].replace(f'_{crosspol}.tif', f'_{copol}.tif'),
            'track_number': p['pathNumber'],
        }
        for p in properties
    ]

    df_rtc = gpd.GeoDataFrame(properties_f, geometry=geometry, crs=CRS.from_epsg(4326))

    # Ensure dual polarization
    df_rtc['jpl_burst_id'] = df_rtc['opera_id'].map(lambda bid: bid.split('_')[3])
    df_rtc = df_rtc[df_rtc.polarizations == polarizations].reset_index(drop=True)

    # Sort by burst_id and acquisition date
    df_rtc = df_rtc.sort_values(by=['jpl_burst_id', 'acq_dt']).reset_index(drop=True)

    # Remove duplicates from time series
    df_rtc['dedup_id'] = df_rtc.opera_id.map(lambda id_: '_'.join(id_.split('_')[:5]))
    df_rtc = df_rtc.drop_duplicates(subset=['dedup_id']).reset_index(drop=True)
    df_rtc = df_rtc.drop(columns=['dedup_id'])

    return df_rtc


def get_rtc_s1_temporal_group_metadata(
    mgrs_tile_ids: list[str],
    track_numbers: list[int],
    n_images_per_burst: int = 1,
    start_acq_dt: datetime | str = None,
    stop_acq_dt: datetime | str = None,
    max_variation_seconds: float = None,
) -> gpd.GeoDataFrame:
    """
    Meant for acquiring a pre-image or post-image set from MGRS tiles for a given S1 pass.

    Obtains the most recent burst image set within a date range.

    For acquiring a post-image set, we provide the keyword argument max_variation_seconds to ensure the latest
    acquisition of are within the latest acquisition time from the most recent burst image. If this is not provided,
    you will get the latest burst image product for each burst within the allowable date range. This could yield imagery
    collected on different dates for the burst_ids provided.

    For acquiring a pre-image set, we use n_images_per_burst > 1. We get the latest n_images_per_burst images for each
    burst and there can be different number of images per burst for all the burst supplied and/or the image
    time series can be composed of images from different dates.

    Note we take care of the equator edge cases in LUT of the MGRS/burst_ids, so only need to provide 1 valid track
    number in pass.

    Parameters
    ----------
    mgrs_tile_ids: list[str]
    track_numbers: list[int]
    start_acq_dt: datetime | str
    stop_acq_dt : datetime
    max_variation_seconds : float, optional
    n_images_per_burst : int, optional

    Returns
    -------
    gpd.GeoDataFrame
    """
    if len(track_numbers) > 2:
        raise ValueError('Cannot handle more than 2 track numbers.')
    if (len(track_numbers) == 2) and (abs(track_numbers[0] - track_numbers[1]) > 1):
        raise ValueError('Two track numbers that are not consecutive were provided.')
    burst_ids = get_burst_ids_in_mgrs_tiles(mgrs_tile_ids, track_numbers=track_numbers)
    if not burst_ids:
        raise ValueError('No burst ids found for the provided MGRS tile and track numbers.')

    if (n_images_per_burst == 1) and (max_variation_seconds is None):
        warn(
            'No maximum variation in acq dts provided although n_images_per_burst is 1. '
            'This could yield imagery collected on '
            'different dates for the burst_ids provided.',
            category=UserWarning,
        )
    df_rtc = get_rtc_s1_ts_metadata_by_burst_ids(
        burst_ids,
        start_acq_dt=start_acq_dt,
        stop_acq_dt=stop_acq_dt,
    )
    columns = df_rtc.columns
    # Assumes that each group is ordered by date (earliest first and most recent last)
    df_rtc = df_rtc.groupby('jpl_burst_id').tail(n_images_per_burst).reset_index(drop=False)
    df_rtc = df_rtc[columns]
    if max_variation_seconds is not None:
        if (n_images_per_burst is None) or (n_images_per_burst > 1):
            raise ValueError('Cannot apply maximum variation in acq dts when n_images_per_burst > 1 or None.')
        max_dt = df_rtc['acq_dt'].max()
        ind = df_rtc['acq_dt'] > max_dt - pd.Timedelta(seconds=max_variation_seconds)
        df_rtc = df_rtc[ind].reset_index(drop=True)
    min_dt = df_rtc['acq_dt'].min()
    df_rtc['pass_id'] = df_rtc['acq_dt'].map(lambda dt: (dt - min_dt).total_seconds() / 86400 // 6)
    df_rtc['acq_date'] = df_rtc.groupby('pass_id')['acq_dt'].transform('min').dt.floor('D')

    df_lut = get_lut_by_mgrs_tile_ids(mgrs_tile_ids)
    df_rtc = pd.merge(
        df_rtc,
        df_lut[['jpl_burst_id', 'mgrs_tile_id', 'acq_group_id_within_mgrs_tile']],
        on='jpl_burst_id',
        how='inner',
    )

    def get_track_token(track_numbers: list[int]) -> str:
        unique_track_numbers = track_numbers.unique().tolist()
        return '_'.join(map(str, sorted(unique_track_numbers)))

    df_rtc['track_token'] = df_rtc.groupby(['mgrs_tile_id', 'acq_group_id_within_mgrs_tile'])['track_number'].transform(
        get_track_token
    )
    df_rtc.drop(columns=['pass_id'], inplace=True)

    rtc_s1_schema.validate(df_rtc)
    df_rtc = reorder_columns(df_rtc, rtc_s1_schema)

    return df_rtc


def get_rtc_s1_ts_metadata_from_mgrs_tiles(
    mgrs_tile_ids: list[str],
    track_numbers: list[int] = None,
    start_acq_dt: str | datetime = None,
    stop_acq_dt: str | datetime = None,
) -> gpd.GeoDataFrame:
    """Get the RTC S1 time series for a given MGRS tile and track number."""
    if isinstance(start_acq_dt, str):
        start_acq_dt = datetime.strptime(start_acq_dt, '%Y-%m-%d')
    if isinstance(stop_acq_dt, str):
        stop_acq_dt = datetime.strptime(stop_acq_dt, '%Y-%m-%d')

    burst_ids = get_burst_ids_in_mgrs_tiles(mgrs_tile_ids, track_numbers=track_numbers)
    df_rtc_ts = get_rtc_s1_ts_metadata_by_burst_ids(burst_ids, start_acq_dt=start_acq_dt, stop_acq_dt=stop_acq_dt)
    if df_rtc_ts.empty:
        mgrs_tiles_str = ','.join(mgrs_tile_ids)
        track_numbers_str = ','.join(map(str, track_numbers))
        warn(f'No RTC S1 metadata found for track {track_numbers_str} in MGRS tile {mgrs_tiles_str}.')
        return gpd.GeoDataFrame()
    return df_rtc_ts


@check_input(rtc_s1_schema, 0)
def agg_rtc_metadata_by_burst_id(df_rtc_ts: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df_agg = (
        df_rtc_ts.groupby('jpl_burst_id')
        .agg(count=('jpl_burst_id', 'size'), earliest_acq_date=('acq_dt', 'min'), latest_acq_date=('acq_dt', 'max'))
        .reset_index()
    )

    return df_agg
