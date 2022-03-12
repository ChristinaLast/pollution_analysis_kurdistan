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
    TARGET_DIR = "local_data"
    START_DATE = "20200101"
    END_DATE = "20220101"
    COUNTRY_SHP = "geo_data/irq_admbnda_adm1_cso_20190603.shp"


@dataclass
class FlaringDescriptorConfig:
    PROCESSED_TARGET_DIR = "iraq_processed_data/local_data"


@dataclass
class FlaringGrouperConfig:
    PROCESSED_TARGET_DIR: str = "iraq_processed_data/local_data"
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


@dataclass
class MethaneLoaderConfig:
    COUNTRY_CODES = ["IQ"]
    CRS: str = "epsg:4326"
    YEAR: int = 2019
    MON_START: int = 7
    DATE_START: int = 13
    YEAR_END: int = 2022
    MON_END: int = 3
    DATE_END: int = 11
    PLACE = "Iraqi Kurdistan, Iraq"
    BASE_FOLDER = "/ee_data"
    METHANE_IMAGE_COLLECTION = "COPERNICUS/S5P/OFFL/L3_CH4"
    METHANE_IMAGE_BAND = [
        "CH4_column_volume_mixing_ratio_dry_air",
    ]
    MODEL_NAME = "CH4"
