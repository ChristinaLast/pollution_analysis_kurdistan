import os
from os.path import isfile, join
from joblib import Parallel, delayed
import pandas as pd

from open_geo_engine.utils.utils import read_csv, write_csv


def concatenate_methane_data(filepath):
    names = [
        "longitude",
        "latitude",
        "time",
        "datetime",
        "CH4_column_volume_mixing_ratio_dry_air",
    ]
    methane_df = read_csv(f"ee_data/{filepath}")
    if list(methane_df.columns) != names:
        return read_csv(f"ee_data/{filepath}", header=names)
    else:
        return methane_df
    # print(methane_df)
    # return methane_df_list.append(methane_df)


def has_header(file, nrows=20):
    df = pd.read_csv(file, header=None, nrows=nrows)
    df_header = pd.read_csv(file, nrows=nrows)
    return tuple(df.dtypes) != tuple(df_header.dtypes)


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
    methane_by_date_df = pd.concat(
        Parallel(n_jobs=-1, backend="multiprocessing", verbose=5)(
            delayed(concatenate_methane_data)(filepath)
            for filepath in methane_filepaths
        )
    )
    print(methane_by_date_df)
    # methane_by_date_df = pd.concat(methane_by_date_list)
    write_csv(
        methane_by_date_df.drop_duplicates(),
        "summarised_data/kurdistan_processed_data/methane_concentrations.csv",
    )
