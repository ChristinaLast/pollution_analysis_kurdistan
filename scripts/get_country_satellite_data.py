import datetime
import ee
import json
import geemap
from joblib import Parallel, delayed
import os


class GetCountrySatelliteData:
    def __init__(
        self,
        geo_dir: json,
        image_folder: str,
        year_start: int,
        mon_start: int,
        date_start: int,
        year_end: int,
        mon_end: int,
        date_end: int,
    ) -> None:
        self.geo_dir = geo_dir
        self.image_folder = image_folder
        self.year_start = year_start
        self.mon_start = mon_start
        self.date_start = date_start
        self.year_end = year_end
        self.mon_end = mon_end
        self.date_end = date_end

    def execute(
        self,
    ):
        ee.Initialize()
        Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(self.execute_for_geojson)(file)
            for file in os.listdir(self.geo_dir)
            if os.path.isfile(os.path.join(self.geo_dir, file))
        )

    def execute_for_geojson(self, file):
        with open(
            os.path.join(self.geo_dir, file),
        ) as f:
            geo_json = json.load(f)
        dates = self.create_dates()
        self.start_process(geo_json, dates)

    def start_process(self, geo_json, dates):
        for i in range(len(dates) - 1):
            s_date = dates[i]
            e_date = dates[i + 1]
            ee.Initialize()
            collection = (
                ee.ImageCollection("MODIS/006/MCD19A2_GRANULES")
                .select("Optical_Depth_047")
                .filterDate(s_date, e_date)
            )
            geojson_feat_col = self._ee_to_feature_collection(geo_json)
            filtered_collection = collection.filter(ee.Filter.bounds(geojson_feat_col))
            img = filtered_collection.mean()
            print(geojson_feat_col.geometry())
            geemap.ee_export_image(
                img,
                filename=f"{self.image_folder}/MODIS_{s_date}_{e_date}.tif",
                scale=90,
                region=geojson_feat_col.geometry(),
                file_per_band=False,
            )

    def create_dates(
        self,
    ):
        start = datetime.date(self.year_start, self.mon_start, self.date_start)
        end = datetime.date(self.year_end, self.mon_end, self.date_end)
        dateList = self._date_range(start, end)
        dates = [str(date) for date in dateList]
        return dates

    def _date_range(self, start, end):
        r = (end + datetime.timedelta(days=1) - start).days
        return [start + datetime.timedelta(days=i) for i in range(0, r, 7)]

    def _ee_to_feature_collection(self, geo_json):
        features = ee.FeatureCollection(geo_json)
        return features


if __name__ == "__main__":
    year_start = 2022
    mon_start = 1
    date_start = 1
    year_end = 2022
    mon_end = 8
    date_end = 26
    geo_dir = "geo_data/geojson"
    folder = "geo_data/aod_data"
    GetCountrySatelliteData(
        geo_dir,
        folder,
        year_start,
        mon_start,
        date_start,
        year_end,
        mon_end,
        date_end,
    ).execute()
