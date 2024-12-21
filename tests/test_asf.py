import pytest

from dist_s1_enumerator.asf import append_pass_data, get_rtc_s1_ts_metadata_by_burst_ids


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


@pytest.mark.integration
def test_appending_mgrs_tiles() -> None:
    # Bay area burst in overlapping mgrs tile area
    burst_id = 'T115-245714-IW1'
    # TODO: mock this data
    # burst from exactly one_pass
    df_rtc_resp = get_rtc_s1_ts_metadata_by_burst_ids([burst_id], start_acq_dt='2024-01-01', stop_acq_dt='2024-01-12')

    # Check that when you append two MGRS tiles you get two separate rows in DF
    df_rtc_formatted_2_rows = append_pass_data(df_rtc_resp, ['10SEG', '10SEH'])
    assert df_rtc_formatted_2_rows.shape[0] == 2

    # Should now only have one row
    df_rtc_formatted_1_row = append_pass_data(df_rtc_resp, ['10SEG'])
    assert df_rtc_formatted_1_row.shape[0] == 1

    df_rtc_formatted_no_rows = append_pass_data(df_rtc_resp, ['22NFF'])
    assert df_rtc_formatted_no_rows.empty
