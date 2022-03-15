import os
from os.path import isfile, join
from joblib import Parallel, delayed
import pandas as pd

from open_geo_engine.utils.utils import read_csv, write_csv


def concatenate_methane_data(filepath, methane_df_list):
    return methane_df_list.append(read_csv(filepath))


if __name__ == "__main__":
    non_flaring_filetypes = [
        ".DS_Store",
    ]
    methane_filepaths = [
        filename
        for filename in os.listdir("ee_data")
        if isfile(join("ee_data", filename))
        and not any(filetype in filename for filetype in non_flaring_filetypes)
    ]
    methane_df_list = []
    methane_by_date_list = Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
        delayed(concatenate_methane_data)(filepath, methane_df_list) for filepath in methane_filepaths
    )
    methane_by_date_df = pd.concat(methane_by_date_list)
    write_csv(methane_by_date_df.drop_duplicates(), "summarised_data/kurdistan_processed_data/methane_concentrations.csv")
