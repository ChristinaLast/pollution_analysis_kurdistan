import os

import pandas as pd
from joblib import Parallel, delayed

from pollution_analysis.config.model_settings import SatelliteLoaderConfig
from pollution_analysis.src.utils.utils import read_csv, write_csv


class AggregateSatelliteData:
    def __init__(self, satellite_value_cols):
        self.satellite_value_cols = satellite_value_cols

    def calculate_monthly_satellite_data(self, filepath):
        satellite_value_list = self._extract_satellite_value_cols()
        satellite_df = read_csv(filepath)
        satellite_df["month_year"] = self._get_month_df_from_timestamp(satellite_df)
        satellite_df = self._remove_fill_values(satellite_df, satellite_value_list)
        satellite_df_grpby = satellite_df.groupby(
            ["longitude", "latitude", "month_year"]
        )
        return satellite_df_grpby[satellite_value_list].mean().reset_index()

    def _extract_satellite_value_cols(self):
        return [ele for ele in self.satellite_value_cols]

    def _get_month_df_from_timestamp(self, satellite_df):
        return pd.to_datetime(satellite_df["datetime"]).dt.to_period("M")

    def _remove_fill_values(self, satellite_df, satellite_value_list):
        for col in satellite_value_list:
            satellite_df = satellite_df[satellite_df[f"{col}"] != -28672]
        return satellite_df


if __name__ == "__main__":
    non_flaring_filetypes = [
        ".DS_Store",
    ]
    satellite_folder = "aod_data/study_regions"

    satellite_filepaths = [
        os.path.join(satellite_folder, filename)
        for filename in os.listdir(f"{satellite_folder}")
        if not any(filetype in filename for filetype in non_flaring_filetypes)
    ]
    satellite_by_month_df = pd.concat(
        Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(
                AggregateSatelliteData(
                    SatelliteLoaderConfig.IMAGE_BAND
                ).calculate_monthly_satellite_data
            )(filepath)
            for filepath in satellite_filepaths
        )
    )
    write_csv(
        satellite_by_month_df,
        "aod_data/aod_monthly_concentrations.csv",
    )
