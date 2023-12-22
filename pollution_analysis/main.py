import click
import ee
from config.model_settings import (
    FlaringDescriptorConfig,
    FlaringGrouperConfig,
    FlaringLoaderConfig,
    SatelliteLoaderConfig,
    FlaringClusterConfig,
)
import pandas as pd
from joblib import Parallel, delayed
from src.flaring_descriptor import FlaringDescriptor
from src.flaring_grouper import FlaringGrouper
from src.flaring_loader import FlaringLoader
from src.flaring_clusterer import FlaringClusterer

from src.satellite_loader import SatelliteLoader
from src.utils.utils import read_csv, write_csv, write_csv_to_geojson, read_gdf


class FlaringLoaderFlow:
    def __init__(self):
        self.flaring_loader_config = FlaringLoaderConfig()

    def execute(self):
        # Trigger the authentication flow.
        flaring_loader = FlaringLoader.from_dataclass_config(
            self.flaring_loader_config
        )

        flaring_loader.execute()


class FlaringDescriptorFlow:
    def __init__(self):
        self.flaring_descriptor_config = FlaringDescriptorConfig()

    def execute(self):
        # Trigger the authentication flow.
        flaring_descriptor = FlaringDescriptor.from_dataclass_config(
            self.flaring_descriptor_config
        )

        flaring_descriptor.execute()


class FlaringSatelliteFetcherFlow:
    def __init__(self, processed_file):
        self.satellite_loader_config = SatelliteLoaderConfig()
        self.processed_file = processed_file

    def execute(self):
        satellite_loader = SatelliteLoader.from_dataclass_config(
            self.satellite_loader_config,
        )
        ee.Authenticate()
        if "csv" in self.processed_file:
            df_iterator = read_csv(
                self.processed_file, chunksize=100, on_bad_lines="skip"
            )
        elif "geojson" in self.processed_file:
            df_iterator = read_gdf(self.processed_file, rows=100)
        # df_iterator = read_gdf(
        #     self.processed_file, rowa=100, on_bad_lines="skip"
        # )
        full_satellite_df = pd.concat(
            Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
                delayed(satellite_loader.execute)(i, chunk)
                for i, chunk in enumerate(df_iterator)
            )
        )
        write_csv(
            full_satellite_df,
            "local_data/grouped_data/ch4_data/CH4_concentrations_full.csv",
        )
        write_csv_to_geojson(
            full_satellite_df,
            "longitude",
            "latitude",
            "local_data/grouped_data/ch4_data/CH4_concentrations_full.csv",
        )


class FlaringGrouperFlow:
    def __init__(self, processed_flaring_file):
        self.flaring_grouper_config = FlaringGrouperConfig()
        self.processed_flaring_file = processed_flaring_file

    def execute(self):
        flaring_grouper = FlaringGrouper.from_dataclass_config(
            self.flaring_grouper_config,
        )

        flaring_grouper.execute(self.processed_flaring_file)


class FlaringClusterFlow:
    def __init__(
        self,
    ):
        self.flaring_cluster_config = FlaringClusterConfig()

    def execute(self):
        flaring_cluster = FlaringClusterer.from_dataclass_config(
            self.flaring_cluster_config,
        )

        flaring_cluster.execute()


@click.command("flaring_loader", help="Load flaring data from local folder")
def load_flaring_data():
    FlaringLoaderFlow().execute()


@click.command(
    "flaring_descriptor",
    help=(
        "Describe temporal changes in flaring in a coutry's preprocessed data"
    ),
)
def describe_flaring_data():
    FlaringDescriptorFlow().execute()


@click.command(
    "group_flaring_data",
    help="Group flaring locations for further analysis",
)
@click.argument("processed_flaring_file")
def group_flaring_data(processed_flaring_file):
    FlaringGrouperFlow(processed_flaring_file).execute()


@click.command(
    "cluster_flaring_data",
    help="Cluster raw flaring locations to group as individual flare",
)
def cluster_flaring_data():
    FlaringClusterFlow().execute()


@click.command(
    "load_satellite_data",
    help=(
        "Get the unique flaring locations at a 1km granularity, and download"
        " all satellite data given a time period"
    ),
)
@click.argument("processed_file")
def load_satellite_data(processed_file):
    FlaringSatelliteFetcherFlow(processed_file).execute()


@click.command(
    "run_pipeline",
    help="Full pipeline",
)
@click.argument("processed_flaring_file")
def run_pipeline(processed_flaring_file):
    FlaringLoaderFlow().execute()
    FlaringDescriptorFlow().execute()
    FlaringGrouperFlow(processed_flaring_file).execute()


@click.group(
    "pollution-analyisis",
    help=(
        "Library aiming to analyise the level and impact of flaring"
    ),
)
@click.pass_context
def cli(ctx):
    ...


cli.add_command(load_flaring_data)
cli.add_command(describe_flaring_data)
cli.add_command(group_flaring_data)
cli.add_command(load_satellite_data)
cli.add_command(run_pipeline)
cli.add_command(cluster_flaring_data)

if __name__ == "__main__":
    cli()
