import os
from pydantic.dataclasses import dataclass
from dataclasses import field
from typing import Dict, Tuple, Sequence
from pydantic import StrictStr


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
    TARGET_DIR = "raw_data"
    START_DATE = "20190701"
    END_DATE = "20220101"
    COUNTRY_SHP = "geo_data/irq_admbnda_adm1_cso_20190603.shp"


@dataclass
class FlaringDescriptorConfig:
    # PROCESSED_TARGET_DIR = "iraq_processed_data/local_data"
    PROCESSED_TARGET_DIR = "iraq_processed_data/unique_flaring_locations_timeseries.csv"
    DESCRIBED_FLARING_DIR = "summarised_data/flare_processed_data"


@dataclass
class FlaringGrouperConfig:
    PROCESSED_TARGET_DIR: str = "processed_data/all_data/raw_data"
    FLARING_COLUMNS_TO_KEEP: Sequence[str] = field(
        default_factory=lambda: [
            "id",
            "id_Key",
            "Date_Proc",
            "Lat_GMTCO",
            "Lon_GMTCO",
            "Date_LTZ",
            "Cloud_Mask",
        ]
    )
    NO_OF_DP = 2
    # leave empty if no timeseries (only unique locations)
    TIMESERIES_COL = "Date_LTZ"


@dataclass
class SatelliteLoaderConfig:
    COUNTRY_CODES = ["IQ"]
    CRS: str = "epsg:4326"
    YEAR: int = 2019
    MON_START: int = 7
    DATE_START: int = 13
    YEAR_END: int = 2022
    MON_END: int = 3
    DATE_END: int = 11
    PLACE = "Iraqi Kurdistan, Iraq"
    BASE_FOLDER = "image_data"
    IMAGE_COLLECTION = "LANDSAT/LC09/C02/T1_L2"
    IMAGE_BAND = [
        "SR_B1",
        "SR_B2",
        "SR_B3",
    ]
    IMAGE_FOLDER = "image_data"
    MODEL_NAME = "LANDSAT"
    LAT_COL = "Lat_GMTCO"
    LON_COL = "Lon_GMTCO"
