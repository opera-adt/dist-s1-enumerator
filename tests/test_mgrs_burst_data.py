import pytest
from shapely.geometry import Point

from dist_s1_enumerator.exceptions import NoMGRSCoverage
from dist_s1_enumerator.mgrs_burst_data import (
    BLACKLISTED_MGRS_TILE_IDS,
    MAX_BURSTS_IN_MGRS_TILE,
    get_burst_ids_in_mgrs_tiles,
    get_burst_table,
    get_lut_by_mgrs_tile_ids,
    get_mgrs_burst_lut,
    get_mgrs_table,
    get_mgrs_tiles_overlapping_geometry,
)


@pytest.mark.parametrize('mgrs_tile_id', get_mgrs_table()['mgrs_tile_id'].sample(10).tolist())
def test_burst_lookup_by_mgrs_tile_id(mgrs_tile_id: str) -> None:
    if mgrs_tile_id in BLACKLISTED_MGRS_TILE_IDS:
        return
    burst_ids = get_burst_ids_in_mgrs_tiles(mgrs_tile_id)
    n = len(burst_ids)
    assert n > 0
    # at high latitudes, there can be a lot of burst_ids!
    assert n <= MAX_BURSTS_IN_MGRS_TILE


@pytest.mark.parametrize('burst_id', get_burst_table()['jpl_burst_id'].sample(10).tolist())
def test_burst_lookup_by_id(burst_id: str) -> None:
    df_burst = get_burst_table(burst_id)
    assert df_burst.columns.tolist() == ['jpl_burst_id', 'geometry']
    assert not df_burst.empty
    assert df_burst.shape[0] == 1


# Adds some problemeatic tiles including one near the equator (`22NFF`)
@pytest.mark.parametrize('mgrs_tile_id', get_mgrs_table()['mgrs_tile_id'].sample(10).tolist() + ['51NTA', '22NFF'])
def test_mgrs_burst_lookups(mgrs_tile_id: str) -> None:
    if mgrs_tile_id in BLACKLISTED_MGRS_TILE_IDS:
        return

    burst_ids_0 = get_burst_ids_in_mgrs_tiles(mgrs_tile_id)

    df_lut = get_lut_by_mgrs_tile_ids(mgrs_tile_id)
    burst_ids_1 = df_lut.jpl_burst_id.unique().tolist()
    assert set(burst_ids_0) == set(burst_ids_1)

    # There should be a unique acq_group for a given track number in an MGRS tile.
    # We verify this extracting a track number from the burst id and then using the LUT.
    # Get a track number from the first burst id
    track_number_token = burst_ids_0[0].split('-')[0]
    track_number = int(track_number_token[1:])
    # Find the associated acq_group_id_within_mgrs_tile for the track number of the first burst id
    acq_id_from_burst_id = df_lut[df_lut.jpl_burst_id == burst_ids_0[0]]['acq_group_id_within_mgrs_tile'].tolist()[0]
    acq_id_from_track_numbers = (
        df_lut[df_lut.track_number == track_number]['acq_group_id_within_mgrs_tile'].unique().tolist()
    )
    acq_id_from_track_number = acq_id_from_track_numbers[0]
    assert len(acq_id_from_track_numbers) == 1
    assert acq_id_from_burst_id == acq_id_from_track_number

    burst_ids_from_pass_1 = get_burst_ids_in_mgrs_tiles([mgrs_tile_id], track_numbers=[track_number])
    burst_ids_from_pass_2 = df_lut[
        df_lut.acq_group_id_within_mgrs_tile == acq_id_from_track_number
    ].jpl_burst_id.tolist()
    assert set(burst_ids_from_pass_1) == set(burst_ids_from_pass_2)


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


def test_get_burst_ids_in_mgrs_tile_explicit_track_number() -> None:
    anti_meridian_tile_id = '01VCK'
    burst_ids_out = get_burst_ids_in_mgrs_tiles(anti_meridian_tile_id, track_numbers=[1])
    burst_ids_expected = [
        'T001-000696-IW1',
        'T001-000697-IW1',
        'T001-000697-IW2',
        'T001-000698-IW1',
        'T001-000698-IW2',
        'T001-000699-IW1',
        'T001-000699-IW2',
        'T001-000700-IW1',
        'T001-000700-IW2',
        'T001-000701-IW1',
        'T001-000702-IW1',
        'T001-000703-IW1',
    ]
    assert burst_ids_out == burst_ids_expected


def test_no_mgrs_coverage() -> None:
    with pytest.raises(NoMGRSCoverage):
        # point in the Atlantic Ocean
        get_mgrs_tiles_overlapping_geometry(Point(-35, 35))


def test_empty_errors() -> None:
    with pytest.raises(ValueError, match='No burst data found for foo'):
        _ = get_burst_table('foo')

    with pytest.raises(ValueError, match='No LUT data found for MGRS tile ids foo, bar'):
        _ = get_lut_by_mgrs_tile_ids(['foo', 'bar'])


def test_get_mgrs_tiles_overlapping_geometry() -> None:
    # unique mgrs tile over Wax Lake, Louisiana
    point = Point(-91.45, 29.5)

    df_mgrs = get_mgrs_tiles_overlapping_geometry(point)

    assert df_mgrs.columns.tolist() == ['mgrs_tile_id', 'utm_epsg', 'utm_wkt', 'geometry']
    assert df_mgrs.shape[0] == 1
    assert df_mgrs.mgrs_tile_id.tolist() == ['15RXN']


def test_mgrs_tile_track_mismatch() -> None:
    with pytest.raises(ValueError, match='Mismatch - no LUT data found for MGRS tile ids 15RXN and track numbers 1.'):
        _ = get_burst_ids_in_mgrs_tiles('15RXN', track_numbers=[1])


def test_blacklist_mgrs_tiles() -> None:
    df_mgrs_all = get_mgrs_table()
    df_mgrs_lut = get_mgrs_burst_lut()

    df_merged = df_mgrs_all.merge(df_mgrs_lut, on='mgrs_tile_id', indicator=True, how='left')
    MGRS_TILES_NOT_IN_DIST_S1 = df_merged[df_merged['_merge'] == 'left_only'].mgrs_tile_id.unique().tolist()
    assert set(BLACKLISTED_MGRS_TILE_IDS) == set(MGRS_TILES_NOT_IN_DIST_S1)


def test_all_bursts_in_lut() -> None:
    df_bursts = get_burst_table()
    df_mgrs_lut = get_mgrs_burst_lut()

    df_merged = df_bursts.merge(df_mgrs_lut, on='jpl_burst_id', indicator=True, how='left')
    burst_ids_not_in_lut = df_merged[df_merged['_merge'] == 'left_only'].jpl_burst_id.unique().tolist()
    assert len(burst_ids_not_in_lut) == 0
