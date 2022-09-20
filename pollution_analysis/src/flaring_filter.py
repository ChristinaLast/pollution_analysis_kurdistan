import pandas as pd


class FlaringFilter:
    @staticmethod
    def filter_high_temp(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter out rows which contain flaring above 1400K

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe with `high temp values` removed
        """

        return (
            df.assign(high_temp=(df.Temp_BB.apply(lambda temp: float(temp) <= 1400)))
            .query("high_temp == False")
            .drop(["high_temp"], axis=1)
        )
