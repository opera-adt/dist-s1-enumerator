from functools import lru_cache
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon


DATA_DIR = Path(__file__).resolve().parent / 'data'


def get_mgrs_burst_lut_path() -> Path:
    parquet_path = DATA_DIR / 'mgrs_burst_lookup_table.parquet'
    return parquet_path


def get_mgrs_data_path() -> Path:
    parquet_path = DATA_DIR / 'mgrs.parquet'
    return parquet_path


def get_burst_data_path() -> Path:
    parquet_path = DATA_DIR / 'jpl_burst_geo.parquet'
    return parquet_path


def get_burst_table(burst_ids: list[str]) -> gpd.GeoDataFrame:
    parquet_path = get_burst_data_path()
    filters = [('jpl_burst_id', 'in', burst_ids)]
    df = gpd.read_parquet(parquet_path, filters=filters)
    return df


def get_lut_by_mgrs_tile_ids(mgrs_tile_ids: str | list[str]) -> gpd.GeoDataFrame:
    if isinstance(mgrs_tile_ids, str):
        mgrs_tile_ids = [mgrs_tile_ids]
    parquet_path = get_mgrs_burst_lut_path()
    filters = [('mgrs_tile_id', 'in', mgrs_tile_ids)]
    df_mgrs_burst_lut = pd.read_parquet(parquet_path, filters=filters)
    return df_mgrs_burst_lut


@lru_cache
def get_mgrs_table() -> gpd.GeoDataFrame:
    path = get_mgrs_data_path()
    df_mgrs = gpd.read_parquet(path)
    return df_mgrs


def get_mgrs_tiles_overlapping_geometry(geometry: Polygon | Point) -> gpd.GeoDataFrame:
    df_mgrs = get_mgrs_table()
    df_mgrs_overlapping = df_mgrs[df_mgrs.intersects(geometry)].reset_index(drop=True)
    return df_mgrs_overlapping


def get_burst_ids_in_mgrs_tiles(mgrs_tile_ids: str | list) -> list:
    df_mgrs_burst_luts = get_lut_by_mgrs_tile_ids(mgrs_tile_ids)
    burst_ids = df_mgrs_burst_luts.jpl_burst_id.unique().tolist()
    return burst_ids


def get_burst_table_from_mgrs_tiles(mgrs_tile_ids: str | list[str]) -> list:
    df_mgrs_burst_luts = get_lut_by_mgrs_tile_ids(mgrs_tile_ids)
    burst_ids = df_mgrs_burst_luts.jpl_burst_id.unique().tolist()
    df_burst = get_burst_table(burst_ids)
    df_burst = pd.merge(
        df_burst,
        df_mgrs_burst_luts[['jpl_burst_id', 'acq_group_id_within_mgrs_tile', 'mgrs_tile_id']],
        how='left',
        on='jpl_burst_id',
    )
    return df_burst
