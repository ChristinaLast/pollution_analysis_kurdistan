import numpy as np
import logging
from joblib import Parallel, delayed
import os
import pandas as pd

from pollution_analysis.src.utils.utils import read_gdf


class ConvertAODtoPM25:
    def __init__(
        self,
        base_dir: str,
        aod_col: str,
        aod_to_pm25_conversion_factor: int,
    ):
        self.base_dir = base_dir
        self.aod_col = aod_col
        self.aod_to_pm25_conversion_factor = aod_to_pm25_conversion_factor

    def execute(self):
        Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(self.execute_for_file)(file)
            for file in os.listdir(self.base_dir)
            if os.path.isfile(os.path.join(self.base_dir, file))
        )

    def execute_for_file(self, file):
        logging.info(f"Converting {file} AOD to Pm2.5")
        gdf = read_gdf(f"{self.base_dir}/{file}")
        gdf = self._convert_aod_values(gdf)
        avg_gdf = self._temporally_average_pm25(gdf)
        avg_gdf.to_file(
            f"{self.base_dir}/{os.path.splitext(file)[0]}_PM25.geojson",
            driver="GeoJSON",
        )

    def _temporally_average_pm25(self, gdf):
        gdf["avg_pm25"] = gdf.groupby(["longitude", "latitude"])["pm25"].transform(
            "mean"
        )
        return gdf

    def _convert_aod_values(self, gdf):
        gdf[f"{self.aod_col}_norm"] = self._norm_data(gdf)
        gdf = self._aod_to_pm25(gdf)
        return gdf

    def _norm_data(self, gdf):
        """Normalising AOD values between 0 and 1"""
        return (gdf[f"{self.aod_col}"] - np.min(gdf[f"{self.aod_col}"])) / (
            np.max(gdf[f"{self.aod_col}"]) - np.min(gdf[f"{self.aod_col}"])
        )

    def _aod_to_pm25(self, gdf):
        """converting AOD to PM25 using conversion factor"""
        gdf[f"{self.aod_col}_norm"] = pd.to_numeric(
            gdf[f"{self.aod_col}_norm"], errors="coerce"
        )

        gdf["pm25"] = gdf.apply(
            lambda row: row[f"{self.aod_col}_norm"]
            * self.aod_to_pm25_conversion_factor,
            axis=1,
        )
        return gdf


if __name__ == "__main__":
    aod_data = "geo_data/aod_data"
    aod_to_pm25_conversion_factor = 128.1298299845
    aod_col = "Optical_Depth_047"
    ConvertAODtoPM25(aod_data, aod_col, aod_to_pm25_conversion_factor).execute()
