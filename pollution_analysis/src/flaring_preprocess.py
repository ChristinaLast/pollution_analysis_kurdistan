import logging
import pandas as pd
from src.flaring_filter import FlaringFilter


class FlaringPreprocess:
    def __init__(
        self,
        filter_high_temp: bool = True,
    ):
        self.filter_high_temp = filter_high_temp

    @classmethod
    def from_options(cls, filters) -> "FlaringPreprocess":
        filter_default = dict.fromkeys(["filter_high_temp"], False)
        for filter_ in filters:
            filter_default[filter_] = True
        return cls(**filter_default)

    def execute(self, training_validation_df: pd.DataFrame, **kwargs):
        preprocessed_training = self.preprocess_data(training_validation_df)

        return preprocessed_training

    def preprocess_data(self, flaring_df: pd.DataFrame):
        if self.filter_high_temp:
            flaring_df = flaring_df.pipe(FlaringFilter.filter_high_temp)
            logging.info(
                f"""Total number of flares left after
                filtering pregnancies : {len(flaring_df)}"""
            )

        return flaring_df
