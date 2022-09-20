import os
from os.path import isfile, join

import pandas as pd
from joblib import Parallel, delayed
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
        print("no header", methane_df)
        return read_csv(f"ee_data/{filepath}", header=names)
    else:
        print("header", methane_df)

        return methane_df


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
            for filepath in methane_filepaths[:10]
        )
    )
    write_csv(
        methane_by_date_df.drop_duplicates(),
        "summarised_data/kurdistan_processed_data/methane_concentrations.csv",
    )
