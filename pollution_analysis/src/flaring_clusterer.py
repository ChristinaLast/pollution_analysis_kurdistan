from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry import MultiPoint, Point
import geopandas as gpd
from sklearn.metrics import silhouette_score
from config.model_settings import FlaringClusterConfig
import pandas as pd
from src.utils.utils import read_csv, write_csv
import numpy as np


class FlaringClusterer:
    def __init__(self, path_to_data, algorithm, hyperparameter_dict):
        self.path_to_data = path_to_data
        self.algorithm = algorithm
        self.hyperparameter_dict = hyperparameter_dict

    @classmethod
    def from_dataclass_config(
        cls, descriptior_config: FlaringClusterConfig
    ) -> "FlaringClusterer":
        return cls(
            path_to_data=descriptior_config.PATH_TO_DATA,
            algorithm=descriptior_config.ALGORITHM,
            hyperparameter_dict=descriptior_config.HYPERPARAMETER_DICT,
        )

    def preprocess_data(
        self,
    ):
        return read_csv(self.path_to_data)

    def evaluate_model(self, model, data):
        labels = model.labels_
        # Silhouette score is only meaningful when there's more than one cluster
        if len(set(labels)) > 1:
            score = silhouette_score(data, labels, metric="euclidean")
        else:
            score = -1
        return score

    def execute(self):
        if self.algorithm == "DBscan":
            preprocessed_df = self.preprocess_data()
            coords = preprocessed_df[["Lat", "Lon"]].values
            kms_per_radian = 6371.0088

            best_score = float("-inf")
            best_params = None
            best_model = None

            for eps in self.hyperparameter_dict["eps"]:
                for min_samples in self.hyperparameter_dict["min_samples"]:
                    db = DBSCAN(
                        eps=eps / kms_per_radian,
                        min_samples=min_samples,
                        algorithm="ball_tree",
                        metric="haversine",
                    ).fit(np.radians(coords))

                    # Here I'm assuming you have an evaluate_model function that
                    # takes the model and data as input and returns a score
                    score = self.evaluate_model(
                        db, preprocessed_df[["Lat", "Lon"]]
                    )

                    if score > best_score:
                        best_score = score
                        best_params = (eps, min_samples)
                        best_model = db

            print("Best score: ", best_score)
            print("Best params: ", best_params)

            # Use the best model to add the labels to the DataFrame
            preprocessed_df["cluster"] = best_model.labels_

            cluster_labels = best_model.labels_
            num_clusters = len(set(cluster_labels))
            clusters = pd.Series(
                [coords[cluster_labels == n] for n in range(num_clusters)]
            )
            print("Number of clusters: {}".format(num_clusters))

            # DBSCAN clustering
            centermost_points = clusters.map(self.get_centermost_point)
            lats, lons = zip(*centermost_points)
            rep_points = pd.DataFrame({"Lon": lons, "Lat": lats})
            preprocessed_df[["weighted_lon", "weighted_lat"]] = rep_points
            preprocessed_df["geometry"] = preprocessed_df.apply(
                lambda x: Point(
                    (float(x.weighted_lon), float(x.weighted_lat))
                ),
                axis=1,
            )

            gpd.GeoDataFrame(preprocessed_df, geometry="geometry").to_file(
                "local_data/grouped_data/texas_20181001_20230710.geojson",
                driver="GeoJSON",
            )

    def get_centermost_point(self, cluster):
        centroid = (
            MultiPoint(cluster).centroid.x,
            MultiPoint(cluster).centroid.y,
        )
        centermost_point = min(
            cluster, key=lambda point: great_circle(point, centroid).m
        )
        return tuple(centermost_point)
