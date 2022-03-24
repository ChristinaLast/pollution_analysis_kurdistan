import geopandas as gpd
from shapely.prepared import prep

from pollution_analysis.utils.utils import read_csv, write_csv


def convert_df_to_gdf(df):
    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Lon_GMTCO, df.Lat_GMTCO))


def remove_non_kurdistan_flares(gdf, kurdistan_shp):
    grid_polygon_prep = prep(kurdistan_shp.geometry[0])
    return gdf.loc[gdf.geometry.apply(lambda p: grid_polygon_prep.contains(p))]


if __name__ == "__main__":
    crs = {"init": "epsg:4326"}  # Creating a Geographic data frame

    flaring_df = read_csv(
        "summarised_data/kurdistan_processed_data/flaring_geometries.csv"
    )
    flaring_gdf = convert_df_to_gdf(flaring_df)
    kurdistan_shp = gpd.read_file("geo_data/kurdistan.shp")
    filtered_flares = remove_non_kurdistan_flares(flaring_gdf, kurdistan_shp)
    write_csv(
        filtered_flares,
        "summarised_data/kurdistan_processed_data/kuristan_flaring_points.csv",
    )
