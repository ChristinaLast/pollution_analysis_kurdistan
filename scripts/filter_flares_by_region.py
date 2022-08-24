import geopandas as gpd
from shapely.prepared import prep

from pollution_analysis.src.utils.utils import read_csv, write_csv


def convert_df_to_gdf(df):
    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Lon, df.Lat))


def remove_non_kurdistan_flares(gdf, all_shp):
    grid_polygon_prep = prep(all_shp.geometry[0])
    return gdf.loc[gdf.geometry.apply(lambda p: grid_polygon_prep.contains(p))]


if __name__ == "__main__":
    crs = {"init": "epsg:4326"}  # Creating a Geographic data frame

    flaring_df = read_csv("processed_data/all_data/iraq_kurdistan_flaring_points.csv")
    flaring_gdf = convert_df_to_gdf(flaring_df)
    all_shp = gpd.read_file("geo_data/kurdistan.geojson")
    filtered_flares = remove_non_kurdistan_flares(flaring_gdf, all_shp)
    write_csv(
        filtered_flares,
        "processed_data/all_data/kurdistan_flaring_points.csv",
    )
