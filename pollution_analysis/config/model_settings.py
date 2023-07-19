import os
from dataclasses import field
from typing import Any, Dict, Sequence

from pydantic.dataclasses import dataclass


@dataclass
class FlaringScraperConfig:
    BASE_URL = os.getenv("EOC_BASE_URL")
    URL: str = "https://eogdata.mines.edu/wwwdata/viirs_products/vnf/v30//VNF_npp_d20211224_noaa_v30.csv.gz"
    PAYLOAD = {
        "inUserName": os.getenv("EOC_USERNAME"),
        "inUserPass": os.getenv("EOC_PASSWORD"),
    }


@dataclass
class FlaringLoaderConfig:
    RAW_DATA_DIR = "raw_data/"
    TARGET_DIR = "processed_data/"
    START_DATE = "20220101"
    END_DATE = "20221126"
    COUNTRY_SHP = "geo_data/algeria.geojson"


@dataclass
class FlaringDescriptorConfig:
    # PROCESSED_TARGET_DIR = "iraq_processed_data/local_data"
    DESCRIBED_FLARING_DIR = "grouped_data/algeria_data"
    PROCESSED_TARGET_DIR = "processed_data/algeria"


@dataclass
class FlaringGrouperConfig:
    PROCESSED_TARGET_DIR: str = "processed_data/algeria/raw_data"
    FLARING_COLUMNS_TO_KEEP: Sequence[str] = field(
        default_factory=lambda: [
            "id",
            "id_Key",
            "Date_Proc",
            "Lat_GMTCO",
            "Lon_GMTCO",
            "Date_LTZ",
            "Cloud_Mask",
            "Temp_BB",
        ]
    )
    NO_OF_DP = 2
    # leave empty if no timeseries (only unique locations)
    TIMESERIES_COL = "Date_LTZ"
    FILTER_DICT: Dict[str, Any] = field(
        default_factory=lambda: dict(
            filter_high_temp=["Temp_BB"],
        ),
    )


@dataclass
class SatelliteLoaderConfig:
    COUNTRY_BOUNDING_BOXES = {
        "US": (
            "United States",
            (-171.791110603, 18.91619, -66.96466, 71.3577635769),
        ),
    }
    CRS: str = "epsg:4326"
    YEAR: int = 2018
    MON_START: int = 10
    DATE_START: int = 1
    YEAR_END: int = 2023
    MON_END: int = 7
    DATE_END: int = 10
    PLACE = "Tezas"
    BASE_FOLDER = "local_data/grouped_data"
    IMAGE_COLLECTION = "COPERNICUS/S5P/OFFL/L3_CH4"
    IMAGE_BAND = [
        "CH4_column_volume_mixing_ratio_dry_air",
        # "Optical_Depth_055",
    ]
    IMAGE_FOLDER = "ch4_data"
    MODEL_NAME = "CH4"
    LAT_COL = "Lat"
    LON_COL = "Lon"


@dataclass
class FlaringClusterConfig:
    PATH_TO_CLUSTERED_DATA: str = (
        "texas_20181001_20230710_0_2_20_clustered_regions.geojson"
    )
    PATH_TO_UNCLUSTERED_DATA: str = "local_data/grouped_data/texas_20181001_20230710_10dp_unclustered.geojson"
    ALGORITHM: str = "DBscan"
    HYPERPARAMETER_DICT: Dict[str, Any] = field(
        default_factory=lambda: dict(
            eps=[
                0.3,
                # 0.75,
                # 1,
                # 1.25,
            ],
            min_samples=[
                10,
                35,
                45,
                60,
            ],
        ),
    )
