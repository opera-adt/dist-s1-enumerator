import pytest

from dist_s1_enumerator.asf import get_rtc_s1_ts_metadata_by_burst_ids


@pytest.mark.integration
@pytest.mark.parametrize(
    'burst_id, crosspol_token, copol_token',
    [
        ('T064-135515-IW1', 'VH', 'VV'),  # socal burst_id - with VV+VH
        ('T090-191605-IW3', 'HV', 'HH'),  # greenland burst_id - with HH+HV
    ],
)
def test_polararization(burst_id: str, crosspol_token: str, copol_token: str) -> None:
    df_rtc_burst = get_rtc_s1_ts_metadata_by_burst_ids([burst_id])
    assert df_rtc_burst.url_crosspol.str.contains(f'_{crosspol_token}.tif').all()
    assert df_rtc_burst.url_copol.str.contains(f'_{copol_token}.tif').all()
