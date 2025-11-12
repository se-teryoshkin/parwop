import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import TYPE_CHECKING, Generator, Callable
import numpy as np

if TYPE_CHECKING:
    import pandas as pd
    from concurrent.futures import Future


def absolute_file_paths(directory):
    for dirpath,_,filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))


def read_file(file_path: str) -> "pd.DataFrame":
    return pd.read_parquet(file_path)


def read_files_parallel(files: list[str], max_parallel: int = -1):
    futures: list["Future"] = list()

    if max_parallel < 0:
        max_parallel = len(files)

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        for file_path in files:
            futures.append(executor.submit(read_file, file_path))

    return pd.concat(
        [future.result() for future in futures],
        ignore_index=True, sort=False, copy=False
    )


def read_files_parallel_generator(files: list[str], max_parallel: int) -> Generator["pd.DataFrame", None, None]:
    for files_batch in [
        files[x:x+max_parallel]
        for x in range(0, len(files), max_parallel)
    ]:
        yield read_files_parallel(files_batch, max_parallel)


def perform_stage_in_process_pool(files: list[str], max_cores: int, func: Callable, tmp_dir: str) -> None:

    futures: list["Future"] = list()
    with ProcessPoolExecutor(max_workers=max_cores) as executor:
        for files_batch in np.array_split(files, max_cores):
            futures.append(
                executor.submit(func, files_batch.tolist(), tmp_dir)
            )

    for future in futures:
        future.result()
