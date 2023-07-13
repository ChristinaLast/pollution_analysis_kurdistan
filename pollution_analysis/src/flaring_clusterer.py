from sklearn.cluster import DBSCAN
from config.model_settings import FlaringClusterConfig
import pandas as pd
from src.utils.utils import read_csv, write_csv


class FlaringClusterer:
    def __init__(self, path_to_data, algorithm):
        self.path_to_data = path_to_data
        self.algorithm = algorithm

    @classmethod
    def from_dataclass_config(
        cls, descriptior_config: FlaringClusterConfig
    ) -> "FlaringClusterer":
        return cls(
            path_to_data=descriptior_config.PATH_TO_DATA,
            algorithm=descriptior_config.ALGORITHM,
        )

    def preprocess_data(
        self,
    ):
        return read_csv(self.path_to_data)

    def execute(self):
        if self.algorithm == "DBscan":
            preprocessed_df = self.preprocess_data()

            # DBSCAN clustering
            db = DBSCAN(eps=0.5, min_samples=5).fit(
                preprocessed_df[["Lon", "Lat"]]
            )

            # Add the labels to the DataFrame
            preprocessed_df["cluster"] = db.labels_

        # If algorithm is not DBSCAN, return df as is
        write_csv(
            preprocessed_df, "local_data/grouped_data/clustered_data.csv"
        )
