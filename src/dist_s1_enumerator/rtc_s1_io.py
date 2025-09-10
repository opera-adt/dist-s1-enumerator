import concurrent.futures
from pathlib import Path

import geopandas as gpd
import requests
from pandera.pandas import check_input
from rasterio.errors import RasterioIOError
from requests.exceptions import HTTPError, RequestException, Timeout
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from tqdm.auto import tqdm

from dist_s1_enumerator.tabular_models import rtc_s1_schema


def generate_rtc_s1_local_paths(
    urls: list[str], data_dir: Path | str, track_token: list, date_tokens: list[str], mgrs_tokens: list[str]
) -> list[Path]:
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    n = len(urls)
    bad_data = [
        (input_name, len(data))
        for (input_name, data) in zip(
            ['urls', 'date_tokens', 'mgrs_tokens', 'track_token'], [urls, date_tokens, mgrs_tokens, track_token]
        )
        if len(data) != n
    ]
    if bad_data:
        raise ValueError(f'Number of {bad_data[0][0]} (which is {bad_data[0][1]}) must match the number of URLs ({n}).')

    dst_dirs = [
        data_dir / mgrs_token / track_token / date_token
        for (mgrs_token, track_token, date_token) in zip(mgrs_tokens, track_token, date_tokens)
    ]
    [dst_dir.mkdir(parents=True, exist_ok=True) for dst_dir in dst_dirs]

    local_paths = [dst_dir / url.split('/')[-1] for (dst_dir, url) in zip(dst_dirs, urls)]
    return local_paths


def append_local_paths(df_rtc_ts: gpd.GeoDataFrame, data_dir: Path | str) -> list[Path]:
    copol_urls = df_rtc_ts['url_copol'].tolist()
    crosspol_urls = df_rtc_ts['url_crosspol'].tolist()
    track_tokens = df_rtc_ts['track_token'].tolist()
    date_tokens = df_rtc_ts['acq_date_for_mgrs_pass'].tolist()
    mgrs_tokens = df_rtc_ts['mgrs_tile_id'].tolist()

    out_paths_copol = generate_rtc_s1_local_paths(copol_urls, data_dir, track_tokens, date_tokens, mgrs_tokens)
    out_paths_crosspol = generate_rtc_s1_local_paths(crosspol_urls, data_dir, track_tokens, date_tokens, mgrs_tokens)
    df_out = df_rtc_ts.copy()
    df_out['loc_path_copol'] = out_paths_copol
    df_out['loc_path_crosspol'] = out_paths_crosspol
    return df_out


def create_download_session(max_workers: int = 5) -> requests.Session:
    """Create a requests session with appropriate settings for downloads.

    Args:
        max_workers: Number of concurrent download threads (used to size connection pool)
    """
    session = requests.Session()
    session.headers.update({'User-Agent': 'dist-s1-enumerator/1.0'})

    # Size connection pool based on concurrent workers
    pool_maxsize = max(max_workers * 2, 10)
    pool_maxsize = min(pool_maxsize, 50)

    adapter = requests.adapters.HTTPAdapter(
        pool_connections=10,
        pool_maxsize=pool_maxsize,
        max_retries=0,  # handle retries with tenacity
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


@retry(
    retry=retry_if_exception_type((ConnectionError, HTTPError, RasterioIOError, Timeout, RequestException)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)
def localize_one_rtc(url: str, out_path: Path, session: requests.Session | None = None) -> Path:
    """Download a single RTC file with retry logic."""
    if out_path.exists():
        return out_path

    if session is None:
        session = create_download_session()

    try:
        with session.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open('wb') as f:
                for chunk in r.iter_content(chunk_size=16384):
                    if chunk:  # filter out keep-alive chunks
                        f.write(chunk)
    except Exception:
        # Clean up partial file on failure
        if out_path.exists():
            out_path.unlink()
        raise
    return out_path


@check_input(rtc_s1_schema, 0)
def localize_rtc_s1_ts(
    df_rtc_ts: gpd.GeoDataFrame,
    data_dir: Path | str,
    max_workers: int = 5,
    tqdm_enabled: bool = True,
) -> gpd.GeoDataFrame:
    df_out = append_local_paths(df_rtc_ts, data_dir)
    urls = df_out['url_copol'].tolist() + df_out['url_crosspol'].tolist()
    out_paths = df_out['loc_path_copol'].tolist() + df_out['loc_path_crosspol'].tolist()

    # Create shared session for connection pooling, sized for concurrent workers
    session = create_download_session(max_workers)

    def localize_one_rtc_with_session(data: tuple) -> Path:
        url, out_path = data
        return localize_one_rtc(url, out_path, session)

    disable_tqdm = not tqdm_enabled
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        _ = list(
            tqdm(
                executor.map(localize_one_rtc_with_session, zip(urls, out_paths)),
                total=len(urls),
                disable=disable_tqdm,
                desc='Downloading RTC-S1 burst data',
                dynamic_ncols=True,
            )
        )
    # For serialization
    df_out['loc_path_copol'] = df_out['loc_path_copol'].astype(str)
    df_out['loc_path_crosspol'] = df_out['loc_path_crosspol'].astype(str)
    return df_out
