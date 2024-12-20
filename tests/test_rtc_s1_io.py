from pathlib import Path

from dist_s1_enumerator.rtc_s1_io import generate_rtc_s1_local_paths


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
