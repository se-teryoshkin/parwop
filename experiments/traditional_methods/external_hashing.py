import os
import shutil
from uuid import uuid4

from experiments.traditional_methods.exp_utils import (
    absolute_file_paths, read_files_parallel_generator,
    read_files_parallel, perform_stage_in_process_pool,
)


MAX_CORES = 2
MAX_BUCKETS = 25                       # We have ~385 GB data and on bucket read we want the entire
                                       # bucket fit in memory. We have 32 GiB memory some memory is reserved
                                       # for overheads.

MAX_PARALLEL_HASH_FILES_PER_CORE = 8  # 1 file is 200_000 records -> ~1.2GB
                                       # Memory limit is 32GiB, also we reserve some memory
                                       # for overheads.

KEYS = ["SOURCE_POINT_NUMBER"]    # SQ1
# KEYS = ["SENSOR_NUMBER"]        # SQ2
# KEYS = ["CROSSLINE"]            # SQ3
# KEYS = ["INLINE", "CROSSLINE"]  # SQ4

INPUT_PATH = "/data"


def hash_stage(files_to_hash: list[str], directory):

    for df in read_files_parallel_generator(files_to_hash, MAX_PARALLEL_HASH_FILES_PER_CORE):

        df["_BUCKET_NUM"] = df[KEYS].apply(tuple, axis=1).apply(hash) % MAX_BUCKETS

        for bucket_num, sub_df in df.groupby("_BUCKET_NUM", sort=False):
            bucket_dir = f'{directory}/{bucket_num}'
            os.makedirs(bucket_dir, exist_ok=True)
            sub_df.drop(columns=["_BUCKET_NUM"]).to_parquet(
                f"{bucket_dir}/{uuid4().hex}.parquet", index=False
            )


def read_bucket(bucket_files):
    return read_files_parallel(bucket_files).sort_values(by=KEYS, ignore_index=True)


def read_stage(buckets_dir):
    buckets = [
        list(absolute_file_paths(dir_path))
        for dir_path, _, files in os.walk(buckets_dir)
        if files
    ]
    for bucket in buckets:
        yield read_bucket(bucket)

    shutil.rmtree(buckets_dir)


def run():
    buckets_dir = f'/tmp/{uuid4().hex}'
    os.makedirs(buckets_dir)

    files = list(absolute_file_paths(INPUT_PATH))
    perform_stage_in_process_pool(files=files, max_cores=MAX_CORES, func=hash_stage, tmp_dir=buckets_dir)

    return read_stage(buckets_dir)


if __name__ == '__main__':
    for _ in run():
        pass
