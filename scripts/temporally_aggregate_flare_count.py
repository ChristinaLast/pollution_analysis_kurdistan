from datetime import datetime

import pandas as pd

from pollution_analysis.src.utils.utils import read_csv, write_csv


class FlaringAggregator:
    def __init__(
        self,
        processed_target_df: pd.DataFrame,
        described_flaring_dir: str,
        time_aggregation: str,
    ):
        self.processed_target_df = processed_target_df
        self.described_flaring_dir = described_flaring_dir
        self.time_aggregation = time_aggregation

    def execute(
        self,
    ):

        monthly_flaring_df = self.calculate_total_flares_per_month(
            self.processed_target_df
        )

        write_csv(
            monthly_flaring_df,
            f"{self.described_flaring_dir}/{self.time_aggregation}_flaring_count.csv",
        )

    def calculate_total_flares_per_month(self, flaring_by_date_df):
        if self.time_aggregation == "date":
            flaring_by_date_df["Flaring_time_str"] = pd.to_datetime(
                flaring_by_date_df["Date_LTZ"]
            ).dt.to_period("D")

            flaring_grpby = (
                flaring_by_date_df.groupby(["Flaring_time_str", "Lat", "Lon"])
                .count()
                .reset_index()
                .rename(columns={"Date_LTZ": "Count"})
            )
            flaring_grpby["Flaring_timestamp"] = flaring_grpby.apply(
                lambda row: self._get_dately_timestamp(row), axis=1
            )

        else:
            flaring_by_date_df["Flaring_time_str"] = pd.to_datetime(
                flaring_by_date_df["Date_LTZ"]
            ).dt.to_period("D")

            flaring_grpby = (
                flaring_by_date_df.groupby(["Flaring_time_str", "Lat", "Lon"])
                .count()
                .reset_index()
                .rename(columns={"Date_LTZ": "Count"})
            )
            flaring_grpby["Flaring_timestamp"] = flaring_grpby.apply(
                lambda row: self._get_monthly_timestamp(row), axis=1
            )
        total_flaring_by_time = flaring_grpby[
            ["Lat", "Lon", "Flaring_time_str", "Flaring_timestamp", "Count"]
        ]
        return total_flaring_by_time

    def _get_year_from_files(self, row):
        """Filter filenames based on IDs and publication dates"""
        return str(row.year)

    def _replace_symbol(row, item):
        return str(item).replace(".", "_")

    def _get_dately_timestamp(self, row):
        return datetime.strptime(str(row.Flaring_time_str), "%Y-%m-%d").timestamp()

    def _get_monthly_timestamp(self, row):
        return datetime.strptime(str(row.Flaring_time_str), "%Y-%m").timestamp()


if __name__ == "__main__":
    flaring_location_in_time_df = read_csv("grouped_data/kurdistan_flares_grouped.csv")
    described_flaring_dir = "processed_data/kurdistan_data"
    FlaringAggregator(
        flaring_location_in_time_df, described_flaring_dir, "date"
    ).execute()
