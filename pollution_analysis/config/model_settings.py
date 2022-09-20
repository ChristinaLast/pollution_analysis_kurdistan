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
    START_DATE = "20220601"
    END_DATE = "20220826"
    COUNTRY_SHP = "geo_data/kurdistan.geojson"


@dataclass
class FlaringDescriptorConfig:
    # PROCESSED_TARGET_DIR = "iraq_processed_data/local_data"
    DESCRIBED_FLARING_DIR = "grouped_data/kurdistan_data"
    PROCESSED_TARGET_DIR = "processed_data/kurdistan_data/raw_data"


@dataclass
class FlaringGrouperConfig:
    PROCESSED_TARGET_DIR: str = "processed_data/russia_data/raw_data"
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
    COUNTRY_CODES = ["IQ"]
    CRS: str = "epsg:4326"
    YEAR: int = 2019
    MON_START: int = 7
    DATE_START: int = 13
    YEAR_END: int = 2022
    MON_END: int = 4
    DATE_END: int = 8
    PLACE = "Iraqi Kurdistan, Iraq"
    BASE_FOLDER = "aod_data"
    IMAGE_COLLECTION = "MODIS/006/MCD19A2_GRANULES"
    IMAGE_BAND = [
        "Optical_Depth_047",
        "Optical_Depth_055",
    ]
    IMAGE_FOLDER = "aod_data"
    MODEL_NAME = "AOD"
    LAT_COL = "y"
    LON_COL = "x"
