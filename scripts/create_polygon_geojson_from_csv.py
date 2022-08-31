import os

import geopandas as gpd
import pandas as pd
from joblib import Parallel, delayed

from pollution_analysis.src.utils.utils import convert_df_to_gdf


class CreatePolygonGeoJSON:
    def __init__(self, satellite_df, x_col, y_col):
        self.satellite_df = satellite_df
        self.x_col = x_col
        self.y_col = y_col

    def create_polygon_geojson(self, geojson_filepath):

        gdf_to_join = convert_df_to_gdf(self.satellite_df, self.x_col, self.y_col)
        gdf = gpd.read_file(geojson_filepath)
        joined_gdf = self._spatial_join(gdf, gdf_to_join)
        joined_gdf["Date"] = pd.to_datetime(joined_gdf["month_year"])
        filepath_suffix = geojson_filepath.split("/")[-1][7:]
        print(filepath_suffix)
        joined_gdf.to_file(
            f"aod_data/aod_polygons/AOD{filepath_suffix}", driver="GeoJSON"
        )

    def _spatial_join(self, gdf, gdf_to_join):
        joined_gdf = gpd.sjoin(gdf, gdf_to_join, how="inner")
        return joined_gdf


if __name__ == "__main__":
    non_flaring_filetypes = [
        ".DS_Store",
    ]
    satellite_df = pd.read_csv("aod_data/monthly_data/aod_monthly_concentrations.csv")

    geojson_folder = "pop_dens_data/polygon_geojson"
    geojson_filepaths = [
        os.path.join(geojson_folder, filename)
        for filename in os.listdir(f"{geojson_folder}")
        if not any(filetype in filename for filetype in non_flaring_filetypes)
    ]

    Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
        delayed(
            CreatePolygonGeoJSON(
                satellite_df, "longitude", "latitude"
            ).create_polygon_geojson
        )(geojson_filepath)
        for geojson_filepath in geojson_filepaths
    )
