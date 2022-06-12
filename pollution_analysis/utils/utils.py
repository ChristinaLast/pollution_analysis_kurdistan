from datetime import timedelta
from typing import Any

import geopandas as gpd
import pandas as pd


def date_range(start, end):
    delta = end - start  # as timedelta
    days = [start + timedelta(days=i) for i in range(delta.days + 1)]
    return days


def read_csv(path: str, **kwargs: Any) -> pd.DataFrame:
    """
    Read csv ensuring that nan's are not parsed
    """

    return pd.read_csv(
        path, sep=",", low_memory=False, encoding="utf-8", na_filter=False, **kwargs
    )


def read_csv_to_gdf(path: str, df_crs, geom_col, **kwargs: Any) -> gpd.GeoDataFrame:
    """
    Read csv to a gpd.GeoDataFrame ensuring that nan's are not parsed
    """
    df = read_csv(path)
    return gpd.GeoDataFrame(df, crs=df_crs, geometry=geom_col, **kwargs)


def write_csv(df: pd.DataFrame, path: str, **kwargs: Any) -> None:
    """
    Write csv to provided path ensuring that the correct encoding and escape
    characters are applied.

    Needed when csv's have text with html tags in it and lists inside cells.
    """
    df.to_csv(
        path,
        index=False,
        na_rep="",
        sep=",",
        line_terminator="\n",
        encoding="utf-8",
        escapechar="\r",
        **kwargs,
    )
