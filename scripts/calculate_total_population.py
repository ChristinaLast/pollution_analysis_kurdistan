import os

import pandas as pd
from joblib import Parallel, delayed

from pollution_analysis.utils.utils import read_csv, write_csv


class CalculateTotalPopulationDensity:
    def __init__(
        self,
        no_of_dp: int,
        timeseries_col: str,
    ):
        self.no_of_dp = no_of_dp
        self.timeseries_col = timeseries_col

    def calculate_population_density_statistics(self, filepath, pop_dens_dir):
        df = read_csv(filepath)
        rounded_df = self.iterate_flaring_locations(df)
        df_with_aggregated_population = self.filter_population_densities(rounded_df)
        write_csv(
            df_with_aggregated_population[
                ["Lon_1dp", "Lat_1dp", "datetime", "population_density"]
            ],
            f"{pop_dens_dir}/rounded_data/{filepath[24:]}",
        )
        return df_with_aggregated_population

    def iterate_flaring_locations(self, df):
        try:
            rounded_df = df.apply(
                lambda row: self._generate_geom_round_lat_lon_values(row),
                axis=1,
            )
        except ValueError:
            print("DataFrame is empty!")
            pass
        return rounded_df

    def filter_population_densities(self, df):
        df[f"{self.timeseries_col}"] = pd.to_datetime(df[f"{self.timeseries_col}"])
        return df.sort_values(self.timeseries_col).drop_duplicates(
            ["Lon_1dp", "Lat_1dp"], keep="last"
        )

    def _generate_geom_round_lat_lon_values(self, row):
        row["Lon_1dp"] = float(round(row["longitude"], self.no_of_dp))
        row["Lat_1dp"] = float(round(row["latitude"], self.no_of_dp))
        return row


if __name__ == "__main__":
    pop_dens_dir = "pop_dens_data/iraq_data"

    unwanted_filetypes = [
        ".DS_Store",
        "rounded_data",
    ]
    population_df_list = []
    no_of_dp = 1
    timeseries_col = "datetime"
    pop_dens_filepaths = [
        os.path.join(pop_dens_dir, filename)
        for filename in os.listdir(f"{pop_dens_dir}")
        if not any(filetype in filename for filetype in unwanted_filetypes)
    ]
    Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
        delayed(
            CalculateTotalPopulationDensity(
                no_of_dp, timeseries_col
            ).calculate_population_density_statistics
        )(filepath, pop_dens_dir)
        for filepath in pop_dens_filepaths
    )
