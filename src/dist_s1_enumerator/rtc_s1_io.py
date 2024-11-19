import concurrent.futures
from pathlib import Path

import backoff
import geopandas as gpd
import requests
from rasterio.errors import RasterioIOError
from requests.exceptions import HTTPError
from tqdm import tqdm


def _generate_rtc_s1_local_paths(
    urls: list[str], data_dir: Path | str, track_token: str, date_tokens: list[str]
) -> list[Path]:
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    if len(urls) != len(date_tokens):
        raise ValueError('Number of URLs and date tokens must be the same.')

    dst_dirs = [data_dir / track_token / date_token for date_token in date_tokens]
    [dst_dir.mkdir(parents=True, exist_ok=True) for dst_dir in dst_dirs]

    local_paths = [dst_dir / url.split('/')[-1] for (dst_dir, url) in zip(dst_dirs, urls)]
    return local_paths


def generate_rtc_s1_local_paths(df_rtc_ts: gpd.GeoDataFrame, data_dir: Path | str) -> list[Path]:
    vv_urls = df_rtc_ts['url_vv'].tolist()
    vh_urls = df_rtc_ts['url_vh'].tolist()
    tracks = df_rtc_ts['track_number'].astype(str).unique().tolist()
    track_token = '_'.join(tracks)
    date_tokens = df_rtc_ts['acq_date'].astype(str).tolist()

    out_paths_vv = _generate_rtc_s1_local_paths(vv_urls, data_dir, track_token, date_tokens)
    out_paths_vh = _generate_rtc_s1_local_paths(vh_urls, data_dir, track_token, date_tokens)
    df_out = df_rtc_ts.copy()
    df_out['loc_path_vv'] = out_paths_vv
    df_out['loc_path_vh'] = out_paths_vh
    return df_out


@backoff.on_exception(
    backoff.expo,
    [ConnectionError, HTTPError, RasterioIOError],
    max_tries=30,
    max_time=60,
    jitter=backoff.full_jitter,
)
def localize_one_rtc(url: str, out_path: Path) -> Path:
    if out_path.exists():
        return out_path

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with out_path.open('wb') as f:
            for chunk in r.iter_content(chunk_size=16384):
                f.write(chunk)
    return out_path


def localize_rtc_s1_ts(
    df_rtc_ts: gpd.GeoDataFrame, data_dir: Path | str, max_workers: int = 5, disable_tqdm: bool = False
) -> list[Path]:
    df_out = generate_rtc_s1_local_paths(df_rtc_ts, data_dir)
    urls = df_out['url_vv'].tolist() + df_out['url_vh'].tolist()
    out_paths = df_out['loc_path_vv'].tolist() + df_out['loc_path_vh'].tolist()

    def localize_one_rtc_p(data: tuple) -> Path:
        return localize_one_rtc(*data)

    # _ = list(tqdm(map(localize_one_rtc_p, zip(urls, out_paths)), total=len(urls), disable=disable_tqdm))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        _ = list(tqdm(executor.map(localize_one_rtc_p, zip(urls, out_paths)), total=len(urls), disable=disable_tqdm))
    # For serliaziation
    df_out['loc_path_vv'] = df_out['loc_path_vv'].astype(str)
    df_out['loc_path_vh'] = df_out['loc_path_vh'].astype(str)
    return df_out
