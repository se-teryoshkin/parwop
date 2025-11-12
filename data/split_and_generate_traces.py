import numpy as np
from numpy.random import uniform
import os
import pandas as pd
from uuid import uuid4


PARQUET_FILE_SIZE = 200_000   # 200_000 have been used for single-node experiments,
                              # for other experiments 300_000 have been used.
DATA_PATH = "/output_dir/multiplied_datasets"
SAVE_DIR = "/output_dir/completed_data"
TRACE_LENGTH = 1501
f32 = np.float32


def process_dataframe(path: str) -> None:

    df = pd.read_parquet(path).sort_values(by="TRACE_SEQUENCE_NUMBER_FILE", kind="mergesort", ignore_index=True)

    processed_rows = 0
    while processed_rows < len(df):

        sub_df: "pd.DataFrame" = df.iloc[processed_rows:processed_rows+PARQUET_FILE_SIZE].copy()
        sub_df["TRACE"] = sub_df["TRACE_SEQUENCE_NUMBER_FILE"].apply(
            lambda _: uniform(-1000, 1000, TRACE_LENGTH).astype(f32)
        )

        filename = f"{DATA_PATH}/{uuid4().hex}.parquet"
        attempts = 10
        while os.path.exists(filename):
            filename = f"{DATA_PATH}/{uuid4().hex}.parquet"
            attempts -= 1
            if attempts == 0:
                raise ValueError(f"Unable to generate filename that is not exists!")

        sub_df.to_parquet(filename, index=False)

        processed_rows += len(sub_df)


def run():

    for filename in os.listdir(DATA_PATH):
        if not filename.endswith(".parquet"):
            continue
        print(f"Processing {filename}")
        process_dataframe(f"{DATA_PATH}/{filename}")


if __name__ == "__main__":
    run()