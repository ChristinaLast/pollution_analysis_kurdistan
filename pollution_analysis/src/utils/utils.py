import os
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
        path,
        sep=",",
        low_memory=False,
        encoding="utf-8",
        na_filter=False,
        **kwargs,
    )


def read_csv_to_gdf(
    path: str, df_crs, geom_col, **kwargs: Any
) -> gpd.GeoDataFrame:
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
        encoding="utf-8",
        escapechar="\r",
        **kwargs,
    )


def write_csv_to_geojson(df, lat_col, lon_col, path, **kwargs: Any) -> None:
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[f"{lon_col}"], df[f"{lat_col}"])
    )
    gdf.to_file(f"{os.path.splitext(path)[0]}.geojson", driver="GeoJSON")


def read_gdf(path, **kwargs: Any) -> gpd.GeoDataFrame:
    return gpd.read_file(
        path,
        driver="GeoJSON",
        **kwargs,
    )


def ee_array_to_df(arr, list_of_bands):
    """Transforms client-side ee.Image.getRegion array to pandas.DataFrame."""
    df = pd.DataFrame(arr)

    # Rearrange the header.
    headers = df.iloc[0]
    df = pd.DataFrame(df.values[1:], columns=headers)

    # Remove rows without data inside.
    df = df[["longitude", "latitude", "time", *list_of_bands]].dropna()

    # Convert the data to numeric values.
    for band in list_of_bands:
        df[band] = pd.to_numeric(df[band], errors="coerce")

    # Convert the time field into a datetime.
    df["datetime"] = pd.to_datetime(df["time"], unit="ms")

    # Keep the columns of interest.
    df = df[["longitude", "latitude", "time", "datetime", *list_of_bands]]

    return df
