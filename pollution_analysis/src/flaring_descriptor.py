import os
import re
from datetime import timedelta
from pathlib import Path

import pandas as pd
from config.model_settings import FlaringDescriptorConfig
from joblib import Parallel, delayed
from src.utils.utils import read_csv, write_csv


class FlaringDescriptor:
    def __init__(self, processed_target_dir: str, described_flaring_dir: str):
        self.processed_target_dir = processed_target_dir
        self.described_flaring_dir = described_flaring_dir

    @classmethod
    def from_dataclass_config(
        cls, descriptior_config: FlaringDescriptorConfig
    ) -> "FlaringDescriptor":

        return cls(
            processed_target_dir=descriptior_config.PROCESSED_TARGET_DIR,
            described_flaring_dir=descriptior_config.DESCRIBED_FLARING_DIR,
        )

    def execute(
        self,
    ):
        total_flaring_by_date_list = Parallel(
            n_jobs=-1, backend="multiprocessing", verbose=5
        )(
            delayed(self.execute_for_year)(dirname, containing_folder, fileList)
            for dirname, containing_folder, fileList in os.walk(
                self.processed_target_dir
            )
        )

        total_flaring_by_date = pd.concat(total_flaring_by_date_list)

        total_flaring_by_week = self._calculate_total_flares_per_week(
            total_flaring_by_date
        )

        write_csv(
            total_flaring_by_week,
            f"{self.described_flaring_dir}/weekly_flaring_count.csv",
        )

        monthly_flaring_df = self._calculate_total_flares_per_month(
            total_flaring_by_date
        )

        write_csv(
            monthly_flaring_df,
            f"{self.described_flaring_dir}/annual_flaring_count.csv",
        )

    def execute_for_year(self, dirname, containing_folder, fileList):
        non_flaring_filetypes = [
            ".DS_Store",
        ]
        flaring_filepaths = [
            os.path.join(dirname, filename)
            for filename in fileList
            if not any(filetype in filename for filetype in non_flaring_filetypes)
        ]
        df_list = []
        for filepath in flaring_filepaths:
            df_list.append(self._calculate_total_flares_per_date(filepath))

        try:
            monthly_flaring_df = pd.concat(df_list)

            Path(
                f"{self.described_flaring_dir}/{self._get_year_from_files(filepath)}"
            ).mkdir(parents=True, exist_ok=True)

            write_csv(
                monthly_flaring_df,
                f"{self.described_flaring_dir}/{self._get_year_from_files(filepath)}/ {self._get_year_month_from_files(filepath)}_total_flaring_count.csv",
            )
            return monthly_flaring_df
        except ValueError:
            pass

    def _calculate_total_flares_per_date(self, filepath):
        country_flaring_df = read_csv(filepath)
        country_flaring_df["Flaring_date"] = pd.to_datetime(
            country_flaring_df["Date_Mscan"]
        ).dt.date
        flaring_grpby = (
            country_flaring_df.groupby(["Flaring_date"])
            .count()
            .reset_index()
            .sort_values(by="Flaring_date")
        )
        total_flaring_by_date = flaring_grpby[["Flaring_date", "id"]].rename(
            columns={"id": "Count"}
        )

        return total_flaring_by_date

    def _calculate_total_flares_per_month(self, flaring_by_date_df):
        flaring_by_date_df["Flaring_month"] = pd.to_datetime(
            flaring_by_date_df["Flaring_date"]
        ).dt.to_period("M")
        flaring_monthly_grpby = (
            flaring_by_date_df.groupby(["Flaring_month"])
            .sum()
            .reset_index()
            .sort_values(by="Flaring_month")
        )
        total_flaring_by_month = flaring_monthly_grpby[["Flaring_month", "Count"]]
        return total_flaring_by_month

    def _calculate_total_flares_per_week(self, flaring_by_date_df):
        flaring_by_date_df["week_beginning"] = flaring_by_date_df.apply(
            lambda row: row.Flaring_date - timedelta(days=row.Flaring_date.weekday()),
            axis=1,
        )

        flaring_weekly_grpby = (
            flaring_by_date_df.groupby(["week_beginning"])
            .sum()
            .reset_index()
            .sort_values(by="week_beginning")
        )
        return flaring_weekly_grpby

    def _get_year_month_from_files(self, filepath):
        """Filter filenames based on IDs and publication dates"""
        return str(re.findall("\d+", filepath)[1][:6])

    def _get_year_from_files(self, filepath):
        """Filter filenames based on IDs and publication dates"""
        return str(re.findall("\d+", filepath)[1][:4])
