import os
import copy
from datetime import datetime
from clickhouse_driver import Client
import time
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor


RESULTS_PATH = "/mnt/nfs/parwop/experiments/single-node/clickhouse/results"
TABLE = "default.seismic_data"

PARTITION_BY = ["SOURCE_POINT_NUMBER"]; ORDER_BY = ["SENSOR_NUMBER"]    # SQ1
# PARTITION_BY = ["SENSOR_NUMBER"]; ORDER_BY = ["SOURCE_POINT_NUMBER"]  # SQ2
# PARTITION_BY = ["CROSSLINE"]; ORDER_BY = ["INLINE", "DISTANCE"]       # SQ3
# PARTITION_BY = ["INLINE", "CROSSLINE"]; ORDER_BY = ["DISTANCE"]       # SQ4

CH_HOSTS = (
    "node6.bdcl:9000",
    "node9.bdcl:9000",
    "node22.bdcl:9000",
    "node23.bdcl:9000",
    "node24.bdcl:9000",
    "node26.bdcl:9000",
    "node27.bdcl:9000",
    "node28.bdcl:9000",
)

MAX_CORES = 16
MEMORY_LIMIT_GIB = 128
MEMORY_LIMIT = MEMORY_LIMIT_GIB * 1024*1024*1024  # 128 GiB

BLK_SIZE_BYTES = int(MEMORY_LIMIT / 12)
BLK_SIZE = int(BLK_SIZE_BYTES / (7 * 1024))

TOTAL_ORDER_KEYS = ','.join(PARTITION_BY + ORDER_BY)

CREATE_TABLE_TEMPLATE = f"""
CREATE TABLE {{new_table}} AS {TABLE}
ENGINE = MergeTree()
ORDER BY ({TOTAL_ORDER_KEYS})
SETTINGS
    max_bytes_to_merge_at_max_space_in_pool={BLK_SIZE_BYTES}
"""

CREATE_DISTR_TABLE_TEMPLATE = f"""
CREATE TABLE {{new_table}}_distr AS {TABLE}
ENGINE = Distributed(experimental_cluster, default, {{new_table}})
"""

INSERT_QUERY_TEMPLATE = f"""
INSERT INTO {{new_table}} SELECT * FROM {TABLE}
SETTINGS
    max_threads={MAX_CORES},
    max_insert_block_size={BLK_SIZE},
    min_insert_block_size_rows={BLK_SIZE},
    min_insert_block_size_bytes={BLK_SIZE_BYTES}
"""


PKEYS = ','.join(PARTITION_BY)
OKEYS = ','.join(ORDER_BY)

QUERY_TEMPLATE = f"""
CREATE OR REPLACE TABLE null_table
ENGINE = Null AS
SELECT *,
       row_number() OVER w AS row_num
FROM (SELECT * FROM {{new_table}}_distr ORDER BY {TOTAL_ORDER_KEYS})
WINDOW w AS (PARTITION BY {PKEYS} ORDER BY {OKEYS})
SETTINGS
    max_threads={MAX_CORES},
    max_memory_usage={MEMORY_LIMIT}
"""


DROP_TABLE_TEMPLATE = f"DROP TABLE IF EXISTS {{new_table}}"
DROP_DISTR_TABLE_TEMPLATE = f"DROP TABLE IF EXISTS {{new_table}}_distr"


TIMEOUT_SETTINGS = {
    "sync_request_timeout": 360_000,
    "receive_data_timeout": 360_000_000,
    "receive_timeout": 360_000,
    "send_timeout": 360_000,
    "http_receive_timeout": 360_000,
    "http_send_timeout": 360_000,
    "send_receive_timeout": 360_000
}


def run_all(executor, clients, query, query_id):
    futures = [
        executor.submit(run_query, client=client, query=query, query_id=query_id)
        for client in clients
    ]

    results = list()

    for future in concurrent.futures.as_completed(futures):
        results.append(future.result())

    return results


def run_query(client, query, query_id):
    try:
        return client.execute(
            query,
            query_id=query_id,
            settings=copy.copy(TIMEOUT_SETTINGS)
        )
    except Exception as e:
        raise e


if __name__ == "__main__":

    executor = ThreadPoolExecutor(max_workers=16)

    clients = [
        Client(
            host=hostport.split(":")[0],
            port=int(hostport.split(":")[-1]),
            settings=copy.copy(TIMEOUT_SETTINGS),
            send_receive_timeout=TIMEOUT_SETTINGS["send_receive_timeout"]
        )
        for hostport in CH_HOSTS
    ]

    start_timestamp = int(datetime.utcnow().timestamp() * 1000)

    group_id = f"{start_timestamp}_{'_'.join(PARTITION_BY)}_{MEMORY_LIMIT_GIB}_d_overwrite"

    query_results_path = f"{RESULTS_PATH}/{group_id}"

    os.makedirs(query_results_path)

    total_group_start = datetime.utcnow()

    create_query_id = f"{group_id}_create_table"
    create_query = CREATE_TABLE_TEMPLATE.format(new_table=group_id)

    create_distr_query_id = f"{group_id}_create_distr_table"
    create_distr_query = CREATE_DISTR_TABLE_TEMPLATE.format(new_table=group_id)

    insert_query_id = f"{group_id}_insert"
    insert_query = INSERT_QUERY_TEMPLATE.format(new_table=group_id)

    select_query_id = f"{group_id}_select"
    select_query = QUERY_TEMPLATE.format(new_table=group_id)

    drop_query_id = f"{group_id}_drop"
    drop_query = DROP_TABLE_TEMPLATE.format(new_table=group_id)

    drop_distr_query_id = f"{group_id}_drop"
    drop_distr_query = DROP_DISTR_TABLE_TEMPLATE.format(new_table=group_id)

    start = datetime.utcnow()

    print(f"[{start}] Execute {create_query_id}")
    _ = run_all(executor=executor, clients=clients, query_id=create_query_id, query=create_query)

    print(f"[{start}] Execute {create_distr_query_id}")
    _ = run_all(executor=executor, clients=clients, query_id=create_distr_query_id, query=create_distr_query)

    print(f"[{start}] Execute {insert_query_id}")
    try:

        _ = run_all(executor=executor, clients=clients, query_id=insert_query_id, query=insert_query)

        print(f"[{datetime.utcnow()}] Execute {select_query_id}")
        _ = run_query(client=clients[1], query=select_query, query_id=select_query_id)

        finish = datetime.utcnow()

        total_group_finish = datetime.utcnow()

        time.sleep(5)

        print(f"Query is completed in {finish - start}")

        print(f"Total query is completed in {total_group_finish - total_group_start}")

        with open(f"{query_results_path}/total_duration.txt", "w") as f:
            f.write(f"{total_group_finish - total_group_start}\n")
    finally:
        _ = run_all(executor=executor, clients=clients, query_id=drop_distr_query_id, query=drop_distr_query)
        _ = run_all(executor=executor, clients=clients, query_id=drop_query_id, query=drop_query)
