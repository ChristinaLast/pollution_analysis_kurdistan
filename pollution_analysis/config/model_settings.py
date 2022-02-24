import os
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
    TARGET_DIR = "local_data"
    START_DATE = "20200101"
    END_DATE = "20220101"
    COUNTRY_SHP = "geo_data/irq_admbnda_adm1_cso_20190603.shp"


@dataclass
class FlaringDescriptorConfig:
    PROCESSED_TARGET_DIR = "iraq_processed_data/local_data"
