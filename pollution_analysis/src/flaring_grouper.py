import os

import pandas as pd
from config.model_settings import FlaringGrouperConfig
from joblib import Parallel, delayed
from src.utils.utils import read_csv, write_csv


class FlaringGrouper:
    def __init__(
        self,
        flaring_columns_to_keep: str,
        processed_target_dir: str,
        no_of_dp: int,
        timeseries_col: str,
    ):

        self.flaring_columns_to_keep = flaring_columns_to_keep
        self.processed_target_dir = processed_target_dir
        self.no_of_dp = no_of_dp
        self.timeseries_col = timeseries_col

    @classmethod
    def from_dataclass_config(
        cls, grouper_config: FlaringGrouperConfig
    ) -> "FlaringGrouper":

        return cls(
            flaring_columns_to_keep=grouper_config.FLARING_COLUMNS_TO_KEEP,
            processed_target_dir=grouper_config.PROCESSED_TARGET_DIR,
            no_of_dp=grouper_config.NO_OF_DP,
            timeseries_col=grouper_config.TIMESERIES_COL,
        )

    def execute(
        self,
        processed_file,
    ):
        df_list_with_select_columns = Parallel(
            n_jobs=-1, backend="multiprocessing", verbose=5
        )(
            delayed(self.execute_for_year)(dirname, containing_folder, fileList)
            for dirname, containing_folder, fileList in os.walk(
                self.processed_target_dir
            )
        )

        df_with_select_columns = pd.concat(df_list_with_select_columns)

        df_with_only_geom = (
            df_with_select_columns[["Lon", "Lat", self.timeseries_col]]
            # .drop_duplicates()
            .reset_index(drop=True)
        )
        write_csv(
            df_with_only_geom,
            processed_file,
        )
        return df_with_only_geom

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
        if flaring_filepaths:
            for filepath in flaring_filepaths:
                df = self.get_unique_flaring_locations(
                    read_csv(filepath, usecols=self.flaring_columns_to_keep)
                )
                df_list.append(df)

            return pd.concat(df_list)

    def get_unique_flaring_locations(self, df_with_select_columns):
        try:
            df_with_select_columns = df_with_select_columns.apply(
                lambda row: self._generate_geom_round_lat_lon_values(row),
                axis=1,
            )
        except ValueError:
            print("DataFrame is empty!")
            pass
        return df_with_select_columns

    def _generate_geom_round_lat_lon_values(self, row):
        row["Lon"] = float(round(row["Lon_GMTCO"], self.no_of_dp))
        row["Lat"] = float(round(row["Lat_GMTCO"], self.no_of_dp))
        return row
