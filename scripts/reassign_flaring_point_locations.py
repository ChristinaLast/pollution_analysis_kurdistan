from curses import raw
import os
from typing import List
import pandas as pd
import datetime
from datetime.datetime import timedelta
from pollution_analysis.src.utils.utils import read_csv, read_gdf


class ReassignFlaringPointLocations:
    def __init__(
        self,
        base_dir: str,
        geojson_path: str,
        raw_csv_path: str,
        flare_count: int,
        raw_date_col: str,
        date_col: str,
        timestamp_col: str,
        no_of_dp: str,
    ):
        self.base_dir = base_dir
        self.geojson_path = geojson_path
        self.raw_csv_path = raw_csv_path
        self.flare_count = flare_count
        self.raw_date_col = raw_date_col
        self.date_col = date_col
        self.timestamp_col = timestamp_col
        self.no_of_dp = no_of_dp

    def execute(self):
        gdf = read_gdf(f"{self.base_dir}/{self.geojson_path}")
        raw_flare_df = self._read_raw_flare_data(self.raw_csv_path)
        filtered_gdf = self._filter_flaring_count(gdf)
        filtered_gdf = self._create_date_field_from_timestamp(filtered_gdf)
        filtered_gdf = self._create_week_beginning_date(filtered_gdf)
        raw_flare_df = self._create_raw_date_field(raw_flare_df)
        raw_flare_df = self._create_week_beginning_date(raw_flare_df)
        raw_flare_df["week_beginning"] = pd.to_datetime(raw_flare_df["week_beginning"])
        merged_flare_df = self._merge_week_gdfs(raw_flare_df, filtered_gdf)
        merged_week_df = merged_flare_df.set_geometry("geometry_y")

    def _create_date_field_from_timestamp(self, gdf):
        # merge on date first
        gdf[f"{self.date_col}"] = gdf[f"{self.timestamp_col}"].apply(
            lambda x: datetime.datetime.fromtimestamp(int(x)).strftime("%Y-%m-%d")
        )
        gdf[f"{self.date_col}"] = gdf.apply(
            lambda row: datetime.datetime.strptime(row[f"{self.date_col}"], "%Y-%m-%d"),
            axis=1,
        )
        return gdf

    def _create_week_beginning_date(self, gdf):
        gdf["week_beginning"] = gdf.apply(
            lambda row: row[f"{self.date_col}"]
            - timedelta(days=row[f"{self.date_col}"].weekday()),
            axis=1,
        )
        return gdf

    def _create_raw_date_field(self, raw_df):
        raw_df[f"{self.raw_date_col}"] = raw_df[f"{self.raw_date_col}"].apply(
            lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f").date()
        )
        return raw_df

    def _round_raw_location(self, gdf):
        gdf[f"Lon_{self.no_of_dp}dp"] = gdf["Lon"].round(self.no_of_dp)
        gdf[f"Lat_{self.no_of_dp}dp"] = gdf["Lat"].round(self.no_of_dp)
        return gdf

    def _merge_week_gdfs(self, raw_gdf, gdf):
        """merge raw locations (usually calcullated at 6 decimal place, and grouped
        and counted flares (usually at 2 decimal places) to remove the "grid-like" effect"""
        merged_week_df = pd.merge(
            gdf,
            raw_gdf,
            how="inner",
            left_on=["Lon", "Lat", "week_beginning"],
            right_on=[
                f"Lon_{self.no_of_dp}dp",
                f"Lat_{self.no_of_dp}dp",
                "week_beginning",
            ],
        )
        return merged_week_df

    def _filter_flaring_count(self, gdf):
        return gdf[gdf["Count"] >= self.flare_count]

    def _read_raw_flare_data(self):
        return read_csv(self.raw_csv_path)
