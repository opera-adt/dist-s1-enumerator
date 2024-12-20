from pathlib import Path

import pytest

from dist_s1_enumerator.asf import append_pass_data, get_rtc_s1_ts_metadata_by_burst_ids
from dist_s1_enumerator.rtc_s1_io import generate_rtc_s1_local_paths, localize_rtc_s1_ts


def test_generate_rtc_s1_dst_paths() -> None:
    urls = [
        'https://datapool.asf.alaska.edu/RTC/OPERA-S1/OPERA_L2_RTC-S1_T064-135515-IW1_20240818T015035Z_20240818T064742Z_S1A_30_v1.0_VH.tif',
        'https://datapool.asf.alaska.edu/RTC/OPERA-S1/OPERA_L2_RTC-S1_T064-135516-IW1_20240818T015037Z_20240818T064742Z_S1A_30_v1.0_VH.tif',
        'https://datapool.asf.alaska.edu/RTC/OPERA-S1/OPERA_L2_RTC-S1_T173-370321-IW3_20231030T134501Z_20240122T220756Z_S1A_30_v1.0_VH.tif',
        'https://datapool.asf.alaska.edu/RTC/OPERA-S1/OPERA_L2_RTC-S1_T173-370322-IW3_20231030T134503Z_20240122T220756Z_S1A_30_v1.0_VH.tif',
    ]
    track_tokens = ['64', '64', '173', '173']
    date_tokens = ['2024-08-18', '2024-08-18', '2023-10-30', '2023-10-30']
    mgrs_tokens = ['11SLT', '11SLT', '11SMT', '11SMT']
    data_dir = Path('data')

    out_paths = generate_rtc_s1_local_paths(urls, data_dir, track_tokens, date_tokens, mgrs_tokens)
    expected_paths = [
        Path(
            'data/11SLT/64/2024-08-18/OPERA_L2_RTC-S1_T064-135515-IW1_20240818T015035Z_20240818T064742Z_S1A_30_v1.0_VH.tif'
        ),
        Path(
            'data/11SLT/64/2024-08-18/OPERA_L2_RTC-S1_T064-135516-IW1_20240818T015037Z_20240818T064742Z_S1A_30_v1.0_VH.tif'
        ),
        Path(
            'data/11SMT/173/2023-10-30/OPERA_L2_RTC-S1_T173-370321-IW3_20231030T134501Z_20240122T220756Z_S1A_30_v1.0_VH.tif'
        ),
        Path(
            'data/11SMT/173/2023-10-30/OPERA_L2_RTC-S1_T173-370322-IW3_20231030T134503Z_20240122T220756Z_S1A_30_v1.0_VH.tif'
        ),
    ]
    assert out_paths == expected_paths


@pytest.mark.integration
@pytest.mark.parametrize(
    'burst_id, mgrs_tile_id',
    [
        # Bay area bursts (one ascending, one descending) - bursts are in overlapping mgrs tiles
        # so tiles could be swapped; use only one tile so that no too much downloading done!
        ('T115-245714-IW1', '10SEG'),
        ('T035-073251-IW2', '10SEH'),
    ],
)
def test_lookup_and_download_rtc(burst_id: str, mgrs_tile_id: str, tmpdir: Path) -> None:
    df_rtc_resp = get_rtc_s1_ts_metadata_by_burst_ids([burst_id], start_acq_dt='2024-01-01', stop_acq_dt='2024-01-12')
    df_rtc_formatted = append_pass_data(df_rtc_resp, [mgrs_tile_id])
    assert not df_rtc_formatted.empty
    assert df_rtc_formatted.shape[0] == 1

    # Download the data
    localize_rtc_s1_ts(df_rtc_formatted, tmpdir)

    # Check that the data was downloaded and a directory was created for the track number
    track_number = int(burst_id.split('-')[0][1:])
    assert (Path(tmpdir) / mgrs_tile_id / str(track_number)).exists()
