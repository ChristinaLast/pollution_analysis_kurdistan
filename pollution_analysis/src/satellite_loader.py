from typing import Sequence

# from utils.utils import write_csv
from config.model_settings import SatelliteLoaderConfig
from open_geo_engine.src.load_ee_data import LoadEEData
from shapely.geometry import Point


class SatelliteLoader:
    def __init__(
        self,
        crs: str,
        countries: Sequence,
        year: int,
        mon_start: int,
        date_start: int,
        year_end: int,
        mon_end: int,
        date_end: int,
        image_collection: str,
        image_band: str,
        folder: str,
        image_folder: str,
        model_name: str,
        place: str,
        lon_col: str,
        lat_col: str,
    ):
        self.crs = crs
        self.countries = countries
        self.year = year
        self.mon_start = mon_start
        self.date_start = date_start
        self.year_end = year_end
        self.mon_end = mon_end
        self.date_end = date_end
        self.image_collection = image_collection
        self.image_band = image_band
        self.folder = folder
        self.image_folder = image_folder
        self.model_name = model_name
        self.place = place
        self.lon_col = lon_col
        self.lat_col = lat_col

    @classmethod
    def from_dataclass_config(cls, config: SatelliteLoaderConfig) -> "SatelliteLoader":

        return cls(
            crs=config.CRS,
            countries=config.COUNTRY_CODES,
            year=config.YEAR,
            mon_start=config.MON_START,
            date_start=config.DATE_START,
            year_end=config.YEAR_END,
            mon_end=config.MON_END,
            date_end=config.DATE_END,
            image_collection=config.IMAGE_COLLECTION,
            image_band=config.IMAGE_BAND,
            folder=config.BASE_FOLDER,
            image_folder=config.IMAGE_FOLDER,
            model_name=config.MODEL_NAME,
            place=config.PLACE,
            lon_col=config.LON_COL,
            lat_col=config.LAT_COL,
        )

    def execute(self, i, flaring_geometries):
        flaring_geometries["centroid_geometry"] = flaring_geometries.apply(
            lambda row: self.get_point_geometry_from_lat_lon(row), axis=1
        )

        satellite_df = LoadEEData(
            self.countries,
            self.year,
            self.mon_start,
            self.date_start,
            self.year_end,
            self.mon_end,
            self.date_end,
            self.image_collection,
            self.image_band,
            self.folder,
            self.image_folder,
            self.model_name,
            self.place,
        ).execute_for_country(flaring_geometries, save_images=False)

        satellite_df.to_csv(
            f"{self.folder}/{self.model_name}_concentrations_{i}.csv",
            index=False,  # Skip index column
        )
        return satellite_df

    def get_point_geometry_from_lat_lon(self, row):
        return Point(row[self.lon_col], row[self.lat_col])
