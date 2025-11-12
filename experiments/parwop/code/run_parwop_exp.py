from typing import TYPE_CHECKING

from parwop.solver.query import QueryClient
from parwop.solver.dataset import ParquetDataset


if TYPE_CHECKING:
    import pandas as pd


PARTITION_BY = ["SOURCE_POINT_NUMBER"]; ORDER_BY = ["SENSOR_NUMBER"]    # SQ1
# PARTITION_BY = ["SENSOR_NUMBER"]; ORDER_BY = ["SOURCE_POINT_NUMBER"]  # SQ2
# PARTITION_BY = ["CROSSLINE"]; ORDER_BY = ["INLINE", "DISTANCE"]       # SQ3
# PARTITION_BY = ["INLINE", "CROSSLINE"]; ORDER_BY = ["DISTANCE"]       # SQ4


COORDINATOR_ADDRESS = "http://node6.bdcl:8000"
DATA_PATH = "/data"
LOCAL_RUN = True  # Local run or distributed mode

NODE_IDS = [f"worker-{i}" for i in range(1, 9)]

MEMORY_LIMIT = 5_000_000    # Depends on the data structure and available memory.

# In the experiment 1 record is about 6.3 KB.
# Thus, MEMORY_LIMIT for different container memory limits is:
# 32GiB  = ~5_000_000
# 64GiB  = ~10_000_000
# 128GiB = ~20_000_000


def run_local():
    ds1 = ParquetDataset(path="/data")

    query = QueryClient(
        datasets=[ds1],
        partition_by=PARTITION_BY,
        order_by=ORDER_BY,
        memory_limit=MEMORY_LIMIT,
        is_distributed=False,
        worker_kwargs={
            "read_only": True,
            "max_threads": 1,
        }
    )

    return query.run()


def run_distributed():

    query = QueryClient(
        datasets=[
            ParquetDataset(path=DATA_PATH, node_id=node_id)
            for node_id in NODE_IDS
        ],
        partition_by=PARTITION_BY,
        order_by=ORDER_BY,
        memory_limit=MEMORY_LIMIT,
        is_distributed=True,
        coordinator_address=COORDINATOR_ADDRESS,
        worker_kwargs={
            "read_only": True,
            "max_threads": 1,
            "use_external_address_mq_messages": True,
        }
    )

    return query.run()


if __name__ == "__main__":

    if LOCAL_RUN:
        generator = run_local()
    else:
        generator = run_distributed()

    total_received = 0

    # The data is transferred to the client only if "read_only" flag is False
    for batch in generator:
        batch: "pd.DataFrame"
        total_received += len(batch)

    print(f"Total received: {total_received}")
