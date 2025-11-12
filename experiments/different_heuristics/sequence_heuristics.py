from typing import TYPE_CHECKING
import os

if TYPE_CHECKING:
    import pandas as pd

from parwop.solver.dataset import ParquetDataset
from parwop.solver.query import QueryDriver


PARTITION_BY = ["SOURCE_POINT_NUMBER"]; ORDER_BY = ["SENSOR_NUMBER"]    # SQ1
# PARTITION_BY = ["SENSOR_NUMBER"]; ORDER_BY = ["SOURCE_POINT_NUMBER"]  # SQ2
# PARTITION_BY = ["CROSSLINE"]; ORDER_BY = ["INLINE", "DISTANCE"]       # SQ3
# PARTITION_BY = ["INLINE", "CROSSLINE"]; ORDER_BY = ["DISTANCE"]       # SQ4

DATA_PATH = "/data"
MEMORY_LIMIT = 5_000_000

HEURISTIC = "subsets_heuristic"
# HEURISTIC = "sequential"
# HEURISTIC = "random"


SAVE_META = False
METADATA_NAME = f"BPOD_{','.join(PARTITION_BY)}_{MEMORY_LIMIT}_{HEURISTIC}"
SAVE_PATH = f"/mnt/nfs/parwop/metadata/{METADATA_NAME}"


if __name__ == "__main__":

    query = QueryDriver(
        datasets=[ParquetDataset(path=DATA_PATH)],
        partition_by=PARTITION_BY,
        order_by=ORDER_BY,
        memory_limit=MEMORY_LIMIT,
        is_distributed=False,
        solver_kwargs={"func_name": HEURISTIC}
    )

    query.build_plans()

    node_buffers_meta: dict[str, "pd.DataFrame"] = query.get_node_buffers_metadata()

    print(f"Total dumped: {sum([v['write_dump'].sum() for v in node_buffers_meta.values()])}")

    if SAVE_META:

        print(f"{SAVE_PATH=}")
        os.makedirs(SAVE_PATH, exist_ok=True)

        query.get_partitions_metadata().to_parquet(f"{SAVE_PATH}/partitions_meta.parquet")
        query.get_blocks_metadata().to_parquet(f"{SAVE_PATH}/blocks_meta.parquet")
        query.get_datasets_metadata().to_parquet(f"{SAVE_PATH}/datasets_meta.parquet")
        query.get_sequence_metadata().to_parquet(f"{SAVE_PATH}/sequence_meta.parquet")

        for node_id, df in node_buffers_meta.items():
            df.to_parquet(f"{SAVE_PATH}/{node_id}_buffer_meta.parquet")
