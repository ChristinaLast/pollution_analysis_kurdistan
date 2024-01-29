from typing import Sequence

from shapely.geometry import Point
import pandas as pd
from typing import Any, Tuple
from googleapiclient.errors import HttpError

import shapely
import ee
from ee.ee_exception import EEException

import datetime
from src.utils.utils import ee_array_to_df, write_csv
from config.model_settings import SatelliteLoaderConfig
from open_geo_engine.src.load_ee_data import LoadEEData


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
    def from_dataclass_config(
        cls, config: SatelliteLoaderConfig
    ) -> "SatelliteLoader":
        return cls(
            crs=config.CRS,
            countries=config.COUNTRY_BOUNDING_BOXES,
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

    def execute(self, i, locations_gdf):
        locations_gdf["centroid_geometry"] = locations_gdf.apply(
            lambda row: self.get_point_geometry_from_lat_lon(row), axis=1
        )

        ee.Initialize()
        s_datetime, e_datetime = self._generate_start_end_date()
        coords_tup = list(self.countries.items())[0][1][1]

        geom = ee.Algorithms.GeometryConstructors.BBox(
            coords_tup[0], coords_tup[1], coords_tup[2], coords_tup[3]
        )

        s_date = s_datetime.date()
        e_date = e_datetime.date()

        collection = (
            ee.ImageCollection(self.image_collection)
            .filterBounds(geom)
            .filterDate(str(s_date), str(e_date))
            .select(self.image_band)
        )
        locations_gdf = self._get_xy(locations_gdf)
        locations_ee_list = []
        for lon, lat in zip(locations_gdf.x, locations_gdf.y):
            centroid_point = ee.Geometry.Point(lon, lat)
            satellite_centroid_point = (
                self._get_centroid_value_from_collection(
                    collection, centroid_point
                )
            )
            ee_df = ee_array_to_df(satellite_centroid_point, self.image_band)
            if not ee_df.empty:
                locations_ee_list.append(ee_df)
        satellite_df = pd.concat(locations_ee_list)
        satellite_df.to_csv(
            f"{self.folder}/{self.model_name}_{i}_concentrations.csv",
            index=False,  # Skip index column
        )
        return satellite_df

    def _get_centroid_value_from_collection(self, collection, centroid_point):
        try:
            return collection.getRegion(centroid_point, 10).getInfo()
        except (EEException, HttpError):
            print(
                f"""Centroid location {centroid_point}
                table does not match any existing location."""
            )
            pass
        # satellite_df = LoadEEData(
        #     self.countries,
        #     self.year,
        #     self.mon_start,
        #     self.date_start,
        #     self.year_end,
        #     self.mon_end,
        #     self.date_end,
        #     self.image_collection,
        #     self.image_band,
        #     self.folder,
        #     self.image_folder,
        #     self.model_name,
        #     self.place,
        # ).execute_for_country(flaring_geometries, save_images=False)

    def get_point_geometry_from_lat_lon(self, row):
        print(row)
        return Point(row[self.lon_col], row[self.lat_col])

    def _generate_start_end_date(self) -> Tuple[datetime.date, datetime.date]:
        start = datetime.datetime(self.year, self.mon_start, self.date_start)
        end = datetime.datetime(self.year_end, self.mon_end, self.date_end)
        return start, end

    def _date_range(self, start, end) -> Sequence[Any]:
        r = (end + datetime.timedelta(days=1) - start).days
        return [start + datetime.timedelta(days=i) for i in range(0, r, 7)]

    def _generate_dates(self, date_list) -> Sequence[str]:
        return [str(date) for date in date_list]

    def _get_xy(self, locations_gdf):
        try:
            locations_gdf["centroid_geometry"] = locations_gdf[
                "centroid_geometry"
            ].map(shapely.wkt.loads)

        except TypeError:
            pass

        locations_gdf["x"] = locations_gdf.centroid_geometry.map(lambda p: p.x)
        locations_gdf["y"] = locations_gdf.centroid_geometry.map(lambda p: p.y)
        return locations_gdf
