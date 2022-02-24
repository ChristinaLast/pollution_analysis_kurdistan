import io
import pandas as pd
from joblib import Parallel, delayed
import requests
from datetime import date, datetime

from config.model_settings import FlaringScraperConfig
from utils.utils import date_range


class FlaringScraper:
    def __init__(self, url: str, dates: list, payload: dict):
        self.dates = dates
        self.url = url
        self.payload = payload

    @classmethod
    def from_dataclass_config(
        cls, scraper_config: FlaringScraperConfig
    ) -> "FlaringScraper":

        return cls(
            dates=scraper_config.DATES,
            url=scraper_config.URL,
            payload=scraper_config.PAYLOAD,
        )

    def execute(
        self,
    ):
        Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(self.execute_for_dates) for date in self.dates if self.date_filter()
        )

    def execute_for_dates(self):

        if self.years:
            for year in self.year:
                print(f"Extracting data for {year} from {self.url}")
                self.scrape_url(self.url)
        else:
            all_years = [2018, 2019, 2020, 2021]
            for year in all_years:
                self.scrape_url(self.url)

    def date_filter(self) -> bool:
        """Filter urls based on month and year

        Args:
            url (str): The filename to filter
            dates (list, optional): A list of months to filter for.
                Format: YYYYMMDD-YYYYMMDD. Defaults to 20210101.
        """
        (
            start_year,
            start_month,
            start_day,
            end_year,
            end_month,
            end_day,
        ) = self._segment_date(self.dates)

        start_date = datetime(start_year, start_month, start_day)
        end_date = datetime(end_year, end_month, end_day)

        for date in date_range(start_date, end_date):
            print(datetime.strftime("%Y%m%d"))
        return True

    def scrape_url(self):
        # Use 'with' to ensure the session context is closed after use.

        with requests.Session() as s:
            p = s.post("LOGIN_URL", data=payload)
            # print the html returned or something more intelligent to see if it's a successful login page.
            # An authorised request.
            r = s.get(self.url)

        response = requests.get(self.url)
        content = response.content
        print(type(content))
        df = pd.read_csv(
            io.BytesIO(content),
            sep=",",
            compression="gzip",
            index_col=0,
            quotechar='"',
        )
        print(df.head())

    def _segment_date(self):
        try:
            start, end = self.dates.split("-")
            start_date = datetime.strptime(start, "%Y%m%d")
            end_date = datetime.strptime(end, "%Y%m%d")
            return (
                start_date.year,
                start_date.month,
                start_date.day,
                end_date.year,
                end_date.month,
                end_date.day,
            )
        except AttributeError:
            start_date = datetime.strptime(self.dates, "%Y%m%d")
            return (
                start_date.year,
                start_date.month,
                start_date.day,
                date.today().year,
                date.today().month,
                date.today().day,
            )
