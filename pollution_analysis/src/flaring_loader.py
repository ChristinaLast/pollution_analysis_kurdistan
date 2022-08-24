import os
import re
from pathlib import Path

import geopandas as gpd
from config.model_settings import FlaringLoaderConfig
from joblib import Parallel, delayed
from src.utils.utils import read_csv


class FlaringLoader:
    def __init__(
        self, target_dir: str, start_date: str, end_date: str, country_shp: str
    ):
        self.target_dir = target_dir
        self.start_date = start_date or self._get_dates_from_files()
        self.end_date = end_date or self._get_dates_from_files()
        self.country_shp = country_shp

    @classmethod
    def from_dataclass_config(
        cls, loader_config: FlaringLoaderConfig
    ) -> "FlaringLoader":

        return cls(
            target_dir=loader_config.TARGET_DIR,
            start_date=loader_config.START_DATE,
            end_date=loader_config.END_DATE,
            country_shp=loader_config.COUNTRY_SHP,
        )

    def execute(
        self,
    ):
        """
        This executes the `FlaringLoader` pipeline.

        This function wraps the `execute_for_year` function
        which iterates through multiple year folders in each
        directory to load the flaring data.

        """
        country_gdf = self._read_gdf()
        Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(self.execute_for_year)(
                country_gdf, dirname, containing_folder, fileList
            )
            for dirname, containing_folder, fileList in os.walk(self.target_dir)
        )

    def execute_for_year(self, gdf, dirname, containing_folder, fileList):
        """
        This executes the `FlaringLoader` pipeline for each year folder
        in the specified directory.

        This function extracts the flares within a specified geometry
        and saves the flares contained within that geometry to a
        csv file.

        Parameters
        ----------
        gdf: GeoJSON
            The filepath to a geometry file that will be sued to select
            flaring locations.
        dirname : string
            The root directory through which you will iterate through
        containing_folder : string
            The folder containing all flating datasets assocated with a
            month and year.
        fileList: List
            A list of strings which are the filenames of each individual
            flaring data file in the `containing_folder`.

        Returns
        -------
        GeoDataFrame
            The flares within the bounds of the original `gdf` geometry.
        """

        non_flaring_filetypes = [
            ".DS_Store",
        ]
        flaring_filepaths = [
            os.path.join(dirname, filename)
            for filename in fileList
            if not any(filetype in filename for filetype in non_flaring_filetypes)
        ]

        for filepath in flaring_filepaths:
            dissolved_gdf = gdf.dissolve()

            flaring_df = self._unzip_to_df(filepath)
            flaring_gdf = self._df_to_gdf(flaring_df)
            country_flaring_gdf = self._select_flares_within_country(
                flaring_gdf, dissolved_gdf
            )
            Path(f"processed_data/kurdistan_data/{dirname}").mkdir(
                parents=True, exist_ok=True
            )
            country_flaring_gdf.to_csv(
                f"processed_data/kurdistan_data/{dirname}/{self._get_dates_from_files(filepath)}.csv"
            )

    def _unzip_to_df(self, filepath):
        try:
            csv = read_csv(filepath, error_bad_lines=False)
        except UnicodeError:
            print(filepath)
        return csv

    def _read_gdf(self):
        gdf = gpd.read_file(self.country_shp)
        gdf.set_crs(epsg=4326, inplace=True)
        return gdf

    def _df_to_gdf(self, flaring_df):
        return gpd.GeoDataFrame(
            flaring_df,
            crs=4326,
            geometry=gpd.points_from_xy(flaring_df.Lon_GMTCO, flaring_df.Lat_GMTCO),
        )

    def _get_xy(self, flaring_df):
        flaring_df["x"] = flaring_df.centroid_geometry.map(lambda p: p.x)
        flaring_df["y"] = flaring_df.centroid_geometry.map(lambda p: p.y)
        return flaring_df

    def _select_flares_within_country(self, flaring_gdf, gdf):
        assert flaring_gdf.crs == gdf.crs

        return gpd.sjoin(flaring_gdf, gdf, op="within", how="inner")

    def _get_dates_from_files(self, filepath):
        """Filter filenames based on IDs and publication dates"""
        return str(re.search("([0-9]{4}[0-9]{2}[0-9]{2})", filepath).group(0))
