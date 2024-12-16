import pytest
from shapely.geometry import Point

from dist_s1_enumerator.exceptions import NoMGRSCoverage
from dist_s1_enumerator.mgrs_burst_data import (
    get_burst_ids_in_mgrs_tiles,
    get_burst_table,
    get_lut_by_mgrs_tile_ids,
    get_mgrs_table,
    get_mgrs_tiles_overlapping_geometry,
)


@pytest.mark.parametrize('mgrs_tile_id', get_mgrs_table()['mgrs_tile_id'].sample(10).tolist())
def test_burst_lookup_by_mgrs_tile_id(mgrs_tile_id: str) -> None:
    burst_ids = get_burst_ids_in_mgrs_tiles(mgrs_tile_id)
    n = len(burst_ids)
    assert n > 0
    # at high latitudes, there can be a lot of burst_ids!
    assert n <= 300


@pytest.mark.parametrize('burst_id', get_burst_table()['jpl_burst_id'].sample(10).tolist())
def test_burst_lookup_by_id(burst_id: str) -> None:
    df_burst = get_burst_table(burst_id)
    assert df_burst.columns.tolist() == ['jpl_burst_id', 'geometry']
    assert not df_burst.empty
    assert df_burst.shape[0] == 1


@pytest.mark.parametrize('mgrs_tile_id', get_mgrs_table()['mgrs_tile_id'].sample(10).tolist())
def test_mgrs_burst_lookups(mgrs_tile_id: str) -> None:
    burst_ids_0 = get_burst_ids_in_mgrs_tiles(mgrs_tile_id)

    df_lut = get_lut_by_mgrs_tile_ids(mgrs_tile_id)
    burst_ids_1 = df_lut.jpl_burst_id.unique().tolist()
    assert set(burst_ids_0) == set(burst_ids_1)

    track_number_token = burst_ids_0[0].split('-')[0]
    track_number = int(track_number_token[1:])
    burst_ids_fixed_track = [burst_id for burst_id in burst_ids_0 if burst_id.split('-')[0] == track_number_token]

    burst_ids_fixed_track_1 = get_burst_ids_in_mgrs_tiles([mgrs_tile_id], track_numbers=[track_number])
    assert set(burst_ids_fixed_track) == set(burst_ids_fixed_track_1)

    df_lut = get_lut_by_mgrs_tile_ids(mgrs_tile_id)
    df_lut_fixed_track = df_lut[df_lut.track_number == track_number].reset_index(drop=True)
    burst_ids_pass = df_lut_fixed_track.jpl_burst_id.unique().tolist()
    # near the equator, there can be multiple acq_group_id_within_mgrs_tile for a single track number.
    assert all(burst_id in burst_ids_pass for burst_id in burst_ids_fixed_track)


def test_near_the_equator() -> None:
    """Illustrate how multiple acq_group_id_within_mgrs_tile can exist for a single track number."""
    mgrs_tile_id_near_eq = '22NFF'
    burst_ids_near_eq = get_burst_ids_in_mgrs_tiles(mgrs_tile_id_near_eq)

    burst_ids_pass = [burst_id for burst_id in burst_ids_near_eq if burst_id.split('-')[0] in ['T148', 'T149']]

    burst_ids_pass_1 = get_burst_ids_in_mgrs_tiles(mgrs_tile_id_near_eq, track_numbers=[148])
    assert set(burst_ids_pass) == set(burst_ids_pass_1)

    burst_ids_pass_2 = get_burst_ids_in_mgrs_tiles(mgrs_tile_id_near_eq, track_numbers=[148, 149])
    assert set(burst_ids_pass) == set(burst_ids_pass_2)

    with pytest.raises(
        ValueError,
        match='Multiple acq_group_id_within_mgrs_tile found for mgrs_tile_id 22NFF and track_numbers 148, 170.',
    ):
        _ = get_burst_ids_in_mgrs_tiles(mgrs_tile_id_near_eq, track_numbers=[148, 170])


def test_too_many_track_numbers() -> None:
    with pytest.raises(
        ValueError,
        match='More than 2 track numbers provided. When track numbers are provided, we select data from a single '
        'pass so this is an invalid input.',
    ):
        _ = get_burst_ids_in_mgrs_tiles('22NFF', track_numbers=[1, 2, 3])


def test_no_mgrs_coverage() -> None:
    with pytest.raises(NoMGRSCoverage):
        # point in the Atlantic Ocean
        get_mgrs_tiles_overlapping_geometry(Point(-35, 35))


def test_empty_errors() -> None:
    with pytest.raises(ValueError, match='No burst data found for foo'):
        _ = get_burst_table('foo')

    with pytest.raises(ValueError, match='No LUT data found for MGRS tile ids foo, bar'):
        _ = get_lut_by_mgrs_tile_ids(['foo', 'bar'])
