import os
from uuid import uuid4

import pandas as pd
import pyarrow.parquet as pq
import numpy as np
from typing import Generator

from experiments.traditional_methods.exp_utils import (
    absolute_file_paths, read_files_parallel_generator, perform_stage_in_process_pool,
)


MAX_CORES = 2
MAX_PARALLEL_SORT_FILES_PER_CORE = 8
BATCH_SIZE = 10_000
MAX_MEMORY_USAGE = 5_000_000


PARTITION_BY = ["SOURCE_POINT_NUMBER"]; ORDER_BY = ["SENSOR_NUMBER"]    # SQ1
# PARTITION_BY = ["SENSOR_NUMBER"]; ORDER_BY = ["SOURCE_POINT_NUMBER"]  # SQ2
# PARTITION_BY = ["CROSSLINE"]; ORDER_BY = ["INLINE", "DISTANCE"]       # SQ3
# PARTITION_BY = ["INLINE", "CROSSLINE"]; ORDER_BY = ["DISTANCE"]       # SQ4

KEYS = PARTITION_BY + ORDER_BY

INPUT_PATH = "/data"


def sort_stage(files_to_sort_merge: list[str], directory: str) -> None:
    for df in read_files_parallel_generator(files_to_sort_merge, MAX_PARALLEL_SORT_FILES_PER_CORE):
        df.sort_values(
            by=KEYS, kind="mergesort", ignore_index=True
        ).to_parquet(f'{directory}/{uuid4().hex}', index=False)


def merge_stage(files: list[str]):
    opened = {f: pq.ParquetFile(f).iter_batches(batch_size=BATCH_SIZE) for f in files}
    heads = dict()
    buf = []
    memory_usage = 0

    for f, it in opened.items():
        df = next(it).to_pandas()

        heads[f] = df.iloc[0][KEYS].values.tolist()
        buf.append(df)
        memory_usage += BATCH_SIZE

    while heads:

        if memory_usage >= MAX_MEMORY_USAGE:
            save_threshold = np.array(min(heads.values()))
            tmp_df = pd.concat(
                buf, copy=False, ignore_index=True
            ).sort_values(by=KEYS, kind='mergesort', ignore_index=True)

            mask_array = tmp_df[KEYS].values >= save_threshold
            first_index = np.argmax(mask_array.all(axis=1))
            df_to_save = tmp_df.iloc[:first_index - 1].copy()
            new_df = tmp_df.iloc[first_index:].copy()
            tmp_df = None

            yield df_to_save

            buf = [new_df]
            memory_usage = len(new_df)

        min_key = min(heads, key=heads.get)
        try:
            batch = next(opened[min_key])
            df = batch.to_pandas()
            heads[min_key] = df.iloc[0][KEYS].values.tolist()
            buf.append(df)
            memory_usage += BATCH_SIZE

        except StopIteration:
            opened.pop(min_key)
            heads.pop(min_key)
            if os.path.exists(min_key):
                os.remove(min_key)

    yield pd.concat(buf, ignore_index=True, copy=False).sort_values(by=KEYS, kind='mergesort', ignore_index=True)


def run() -> Generator["pd.DataFrame", None, None]:
    directory = f"/tmp/{uuid4().hex}/sorted"
    os.makedirs(directory, exist_ok=True)

    files = list(os.listdir(INPUT_PATH))
    perform_stage_in_process_pool(files=files, max_cores=MAX_CORES, func=sort_stage, tmp_dir=directory)

    sorted_files: list[str] = list(absolute_file_paths(directory))
    return merge_stage(sorted_files)


if __name__ == "__main__":
    for _ in run():
        pass
