import os

import geopandas as gpd
from typing import List
import pandas as pd
from shapely import wkt
from joblib import Parallel, delayed

from pollution_analysis.src.utils.utils import read_csv, read_gdf


class GroupFlaringClusters:
    def execute(self, clusters_df, max_cluster):
        clustered_df = read_csv(clusters_df)
        clustered_df = clustered_df[clustered_df["cluster"] != -1]
        if max_cluster:
            clustered_df["cluster"] = clustered_df["cluster"] + max_cluster
        else:
            max_cluster = clustered_df.cluster.max()
        clustered_df["geometry"] = clustered_df["geometry"].apply(wkt.loads)
        clustered_df.Date_LTZ = pd.to_datetime(
            clustered_df.Date_LTZ, infer_datetime_format=True
        )
        clustered_gdf = gpd.GeoDataFrame(clustered_df, geometry="geometry")

        return clustered_gdf, max_cluster

    def aggregate_data(self, gdf, gdf_to_join):
        


if __name__ == "__main__":
    flaring_files = [
        "texas_20181001_20230710_0.3_10_unclustered.csv",
        "texas_20181001_20230710_0.2_20_clustered.csv",
    ]
    data_folder = "pop_dens_data/polygon_geojson"
    flaring_cluster_filepaths = [
        os.path.join(data_folder, filename)
        for filename in os.listdir(f"{data_folder}")
        if any(filetype in filename for filetype in flaring_files)
    ]

    all_clusters = pd.concat(
        Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(GroupFlaringClusters().execute)(flaring_cluster_file)
            for flaring_cluster_file in flaring_cluster_filepaths
        )
    )
    all_cluster_grpby = (
        all_clusters.groupby("cluster")
        .agg({"Lat": "mean", "Lon": "mean"})
        .reset_index()
    )
    all_cluster_grpby_gdf = gpd.GeoDataFrame(
        all_cluster_grpby,
        geometry=gpd.points_from_xy(
            x=all_cluster_grpby.Lon, y=all_cluster_grpby.Lat
        ),
    )
    all_clusters_join = pd.merge(
        all_clusters,
        all_cluster_grpby_gdf[["cluster", "geometry"]],
        on="cluster",
    )
    all_clusters_join["centroid_geometry"] = all_clusters_join["geometry_y"]
    all_clusters_join = all_clusters_join.set_geometry("centroid_geometry")
    all_clusters_join = all_clusters_join.drop(columns=["geometry_x"])
    all_clusters_join = all_clusters_join.drop(columns=["geometry_y"])
