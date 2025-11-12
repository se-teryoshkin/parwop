import concurrent.futures
import json
import shutil
import time
from typing import BinaryIO, Hashable, TYPE_CHECKING, Optional, Callable, Literal, Union
from copy import copy
import pandas as pd
import os
import pyarrow as pa
import pyarrow.ipc as ipc
import numpy as np
from datetime import datetime, timezone
import requests
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
import pika
import inspect
from threading import Lock

from parwop.utils import PerformanceLogger, get_rss, trim_memory, PerformanceLoggerDecorator
from parwop.utils.column_names import (
    PARTITION_KEY_COLUMN, PRIMARY_NODE_ID_COLUMN,
    COMPLETE_ITER_COLUMN, SYSTEM_COLUMNS, DUMP_SPLIT_MASK_COLUMN,
)
from parwop.server.discovery import discovery_service
from parwop.settings import DUMPS_PATH, DEBUG, DEFAULT_DUMP_BATCH_SIZE
from parwop.solver.buffer import IterationInfo
from parwop.solver.dataset import DATASET_TYPES
from parwop.solver.partition import Partition
from parwop.solver.block import Block


if TYPE_CHECKING:
    from pyarrow import RecordBatchFileReader, RecordBatch  # noqa
    from parwop.solver.dataset import DatasetType  # noqa
    from pika.exceptions import StreamLostError, ConnectionBlockedTimeout  # noqa
    from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel  # noqa


COMPLEX_TYPES = (list, np.ndarray, dict, set)


class Worker:

    def __init__(self, *,
                 node_id: str,
                 partition_by: list[str],
                 order_by: list[str],
                 query_id: str,
                 datasets_metadata: "pd.DataFrame",
                 blocks_metadata: "pd.DataFrame",
                 partitions_metadata: "pd.DataFrame",
                 node_iterations_metadata: "pd.DataFrame",
                 is_local: bool = False,
                 **kwargs):

        self.node_id = node_id
        self.partition_by = partition_by
        self.order_by = order_by
        self.query_id = query_id
        self.is_local = is_local

        # {iter_num -> iteration_info}
        self.iterations: dict[int, "IterationInfo"] = self.load_iterations_metadata(node_iterations_metadata, node_id)

        # {iter_num -> list[Partition]} - only this node partitions
        this_node_complete_iter_to_partitions: dict[int, list["Partition"]] = {
            iter_num: list()
            for iter_num in self.iterations.keys()
        }

        # {partition_key -> Partition} - only this node partitions
        this_node_partitions: dict[Hashable, "Partition"] = dict()

        # {partition_key -> complete_iteration} - only this node partitions
        this_node_partition_to_complete_iter: dict[Hashable, int] = dict()

        # {partition_key -> node_id}
        partition_nodes: dict[Hashable, str] = dict()

        # {node_ids}
        query_nodes: set[str] = set()

        # {partition_key -> partition}
        partitions: dict[Hashable, "Partition"] = self.load_partitions_metadata(partitions_metadata)

        for key, partition in partitions.items():

            primary_node = partition.primary_node
            partition_nodes[key] = primary_node
            query_nodes.add(primary_node)

            if primary_node == node_id:
                complete_iteration = partition.complete_iteration

                this_node_partitions[key] = partition
                this_node_partition_to_complete_iter[key] = complete_iteration
                this_node_complete_iter_to_partitions[complete_iteration].append(partition)

        self.this_node_partitions = this_node_partitions
        self.this_node_partition_to_complete_iter = this_node_partition_to_complete_iter
        self.partition_nodes = partition_nodes
        self.this_node_complete_iter_to_partitions = this_node_complete_iter_to_partitions
        self.completed_partitions: set[Hashable] = set()

        # {block_id -> block}
        self.blocks: dict[str, "Block"] = self.load_blocks_metadata(blocks_metadata)

        # {dataset_id -> dataset}
        datasets: dict[str, "DatasetType"] = self.load_datasets_metadata(datasets_metadata, self.blocks)
        self.datasets = datasets

        # {block_id -> dataset}
        self.blocks_dataset: dict[str, "DatasetType"] = {
            block_id: datasets[b.dataset_id]
            for block_id, b in self.blocks.items()
        }

        # Limit max output data batch. 0 means no limit.
        self.output_batch_max_size: int = 0 if (v := kwargs.get("output_batch_max_size", None)) is None else v

        # {iter_num -> list[pd.DataFrame]}
        self.shuffle_buffer: dict[int, list["pd.DataFrame"]] = {
            iter_num: list()
            for iter_num, iter_info in self.iterations.items()
            if iter_info.input_shuffle_count > 0
        }
        # {iter_num -> received_count}
        self.shuffle_received_records: dict[int, int] = {
            iter_num: 0
            for iter_num in self.iterations.keys()
        }

        # {iter_num -> list[pd.DataFrame]}
        self.data_buffer: dict[int, list[pd.DataFrame]] = dict()

        self.query_nodes = query_nodes
        self._max_threads = kwargs.get("max_threads", max(1, len(query_nodes)))
        # TODO: Remove executor if there is no shuffle
        self.executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=self._max_threads)

        self.dumps_path = os.path.join(DUMPS_PATH, f"{self.query_id}_{self.node_id}")
        self.keep_dumps = kwargs.get("keep_dumps", False)
        self.parallel_dumping = kwargs.get("parallel_dumping", False)
        self.sortmerge_batch_size = kwargs.get("sortmerge_batch_size", DEFAULT_DUMP_BATCH_SIZE)
        self._init_dump_path()

        self.partitions_data_lock = Lock()

        self.output_batches_lock = Lock()
        self.output_batches: dict[int, "bytes | pd.DataFrame"] = dict()
        self.output_batches_counter: int = 0

        # Set the maximum number of records in the output buffer. 0 - no limit.
        # If the limit is reached, then pause processing until the client to consume it.
        self.max_output_batches_buffer_size: int = kwargs.get("max_output_batches_buffer_size", 0)
        self._current_output_batches_buffer_size: int = 0
        self._slept_for_seconds: int = 0
        self.output_batches_size: dict[int, int] = dict()

        # Just for tracking purpose
        self.total_records: int = sum([m.returned_count for m in self.iterations.values()])
        self.current_returned_records: int = 0

        self.read_only = kwargs.get("read_only", False)
        self.pandas_sort_kind: Literal["quicksort", "mergesort", "heapsort", "stable"] = (  # noqa
            kwargs.get("pandas_sort_algorithm", "mergesort")
        )

        self.is_cancelled = False
        self.is_query_completed = False

        # Set query timeout
        self.query_timeout: int = kwargs.get("query_timeout", 0)
        self.is_query_timed_out: bool = False
        self.start_time: datetime = datetime.now(timezone.utc)

        if kwargs.get("use_external_address_mq_messages", False):
            self.node_address = discovery_service.this_node_status.external_address
            if self.node_address is None:
                raise RuntimeError("External node address is required when use_external_address_mq_messages is True")
        else:
            self.node_address = discovery_service.this_node_status.address

        self._mq_connection: Optional["BlockingConnection"] = None
        self._mq_channel: Optional["BlockingChannel"] = None

        if discovery_service.rabbitmq_host and not self.read_only:
            self._init_rabbitmq_connection()

    def _init_dump_path(self) -> None:
        os.makedirs(self.dumps_path, exist_ok=True)

    def _init_rabbitmq_connection(self):
        self._mq_connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=discovery_service.rabbitmq_host,
                port=discovery_service.rabbitmq_port,
                heartbeat=300
            )
        )
        self._mq_channel = self._mq_connection.channel()
        self._mq_channel.queue_declare(queue=self.query_id)

    @staticmethod
    def collect_block_statistics(partition_by: list[str], datasets: list["DatasetType"]) -> list["Block"]:
        all_blocks: list["Block"] = list()

        for dataset in datasets:
            all_blocks.extend(
                dataset.initialize(partition_by=partition_by, calculate_statistics=True)
            )

        return all_blocks

    @staticmethod
    @PerformanceLoggerDecorator("Load partitions metadata")
    def load_partitions_metadata(df: "pd.DataFrame") -> dict[Hashable, "Partition"]:

        key_sample = df.loc[(0, "key")]
        if not isinstance(key_sample, Hashable):
            raise ValueError("Key should be hashable!")

        partitions: dict[Hashable, "Partition"] = dict()

        # 3x more performant than `.itertuples()`
        for key, records_count, primary_node, complete_iteration, dump_iters_count in zip(
                df["key"].values,
                df["records_count"].values,
                df["primary_node"].values,
                df["complete_iteration"].values,
                df["dump_iters_count"].values,
        ):
            partitions[key] = Partition.load_from_args(
                key=key,
                records_count=records_count,
                primary_node=primary_node,
                complete_iteration=complete_iteration,
                dump_iters_count=dump_iters_count
            )

        return partitions

    @staticmethod
    @PerformanceLoggerDecorator("Load blocks metadata")
    def load_blocks_metadata(df: "pd.DataFrame", with_statistics: bool = False) -> dict[str, "Block"]:
        blocks: dict[str, "Block"] = dict()

        # Make dummy series if no statistics required
        statistics_data = df["block_id"]
        should_be_casted_to_tuple = False

        if with_statistics:
            statistics_data = df["statistic"]

            key_example = statistics_data[0][0]["key"]

            if not isinstance(key_example, Hashable):
                should_be_casted_to_tuple = True

        for block_id, dataset_id, node_id, size_in_records, statistics_raw in zip(
                df["block_id"].values,
                df["dataset_id"].values,
                df["node_id"].values,
                df["size_in_records"].values,
                statistics_data,  # Possible dummy series
        ):

            if with_statistics:
                statistics = pd.DataFrame(statistics_raw.tolist())
                if should_be_casted_to_tuple:
                    statistics["key"] = statistics["key"].apply(tuple)

                statistics.set_index("key", inplace=True)
            else:
                statistics = None

            blocks[block_id] = Block(
                dataset_id=dataset_id,
                block_id=block_id,
                node_id=node_id,
                size_in_records=size_in_records,
                statistics=statistics["count"] if with_statistics else None,
            )

        return blocks

    @staticmethod
    @PerformanceLoggerDecorator("Load Datasets metadata")
    def load_datasets_metadata(df: "pd.DataFrame", blocks: dict[str, "Block"]) -> dict[str, "DatasetType"]:
        datasets: dict[str, "DatasetType"] = dict()
        dataset_blocks: dict[str, list["Block"]] = dict()

        for dataset_id, path, node_id, dataset_type in zip(
                df["dataset_id"].values,
                df["path"].values,
                df["node_id"].values,
                df["dataset_type"].values,
        ):
            datasets[dataset_id] = DATASET_TYPES[dataset_type](
                dataset_id=dataset_id,
                path=path,
                node_id=node_id,
            )
            dataset_blocks[dataset_id] = list()

        for block in blocks.values():
            dataset_blocks[block.dataset_id].append(block)

        for dataset in datasets.values():
            dataset.set_blocks(dataset_blocks[dataset.dataset_id])

        return datasets

    @staticmethod
    @PerformanceLoggerDecorator("Load Iterations metadata")
    def load_iterations_metadata(df: "pd.DataFrame", node_id: str) -> dict[int, "IterationInfo"]:
        return IterationInfo.load_metadata(df, node_id)

    def __del__(self):
        if hasattr(self, "executor") and self.executor is not None:
            try:
                self.executor.shutdown(wait=True)
            except Exception as e:
                print(f"WARNING: During reader finishing (executor shutdown) error occurred: {e}")
                pass
        if hasattr(self, "_mq_channel") and self._mq_channel is not None:
            try:
                self._mq_channel.close()
                self._mq_connection.close()
            except Exception as e:
                print(f"WARNING: During reader finishing (producer closing) error occurred: {e}")
                pass
        if hasattr(self, "dumps_path") and os.path.exists(self.dumps_path):
            if not self.keep_dumps:
                shutil.rmtree(self.dumps_path, ignore_errors=True)

    def batch_on_complete(self, df: "pd.DataFrame"):

        if self.read_only:
            return

        if self.is_local:
            batch_data = df
        else:
            batch_data = self.serialize_data(df)

        batches = self.output_batches
        batches_size = self.output_batches_size
        batch_size = len(df)

        while (
                0 < self.max_output_batches_buffer_size < self._current_output_batches_buffer_size
                and not self.is_cancelled
        ):
            if DEBUG:
                print("Batches limit is reached. Waiting for client read the data...")
            time.sleep(1)
            self._slept_for_seconds += 1

        while self.output_batches_lock.locked():
            if DEBUG:
                print("Waiting for batches lock become released...")
            time.sleep(1)
        self.output_batches_lock.acquire(blocking=False)

        try:
            counter = self.output_batches_counter
            counter += 1

            if counter in batches:
                raise KeyError(f"Batch num already exists: counter={counter}, keys={batches.keys()}")

            batches[counter] = batch_data
            self.output_batches_counter = counter
            batches_size[counter] = batch_size
            self._current_output_batches_buffer_size += batch_size
        finally:
            self.output_batches_lock.release()

        producer = self._mq_channel

        if producer is not None:

            query_id = self.query_id

            body: bytes = json.dumps({
                "node_id": self.node_id,
                "address": f"{self.node_address}/worker/query/{query_id}/batch/{counter}/"
            }).encode("utf-8")

            # Notify Client via RabbitMQ on ready data batch
            try:
                producer.basic_publish(
                    exchange="",
                    routing_key=self.query_id,
                    body=body
                )
            except (pika.exceptions.StreamLostError, pika.exceptions.ConnectionBlockedTimeout) as e:
                print(f"WARNING: RabbitMQ connection was lost ({e}). Reconnecting...")
                self._init_rabbitmq_connection()
                time.sleep(1)
                producer = self._mq_channel
                producer.basic_publish(
                    exchange="",
                    routing_key=self.query_id,
                    body=body
                )

    def get_data_batch(self, batch_num: int) -> Optional["bytes | pd.DataFrame"]:

        while self.output_batches_lock.locked():
            if DEBUG:
                print("Waiting for batches lock (get_batch) become released...")
            time.sleep(1)
        self.output_batches_lock.acquire(blocking=False)

        try:
            batches = self.output_batches

            if batch_num not in batches:
                return None

            data = batches.pop(batch_num, None)
            batch_size = self.output_batches_size.pop(batch_num, 0)

            if batch_num > 0:
                self._current_output_batches_buffer_size -= batch_size
        finally:
            self.output_batches_lock.release()

        return data

    def remove_completed(self,
                         iter_info: "IterationInfo",
                         partition_keys: Optional[set[Hashable]] = None
                         ) -> None:

        iter_num = iter_info.iter_num
        data_buffer = self.data_buffer

        if partition_keys is None:
            remove_all = True
            completed_partitions_count = len(self.this_node_complete_iter_to_partitions[iter_num])
        else:
            remove_all = False
            completed_partitions_count = len(partition_keys)

        if DEBUG:
            print(f"Completed partitions count: {completed_partitions_count}")

        dfs_list = data_buffer.pop(iter_num)

        if DEBUG:
            print(f"Dataframes count: {len(dfs_list)}")

        with PerformanceLogger("Merge dfs list"):
            with PerformanceLogger("Concat and sort result df"):
                output_data: pd.DataFrame = pd.concat(
                    dfs_list, ignore_index=True, axis=0, copy=False
                )

            dfs_list = list()
            _ = trim_memory()

            if not remove_all:

                with PerformanceLogger(f"Partial remove {len(partition_keys)} partitions count"):
                    for is_completed, sub_df in output_data.groupby(
                        output_data[PARTITION_KEY_COLUMN].isin(partition_keys),
                        sort=False
                    ):
                        if is_completed:
                            process_part = copy(sub_df)
                        else:
                            data_buffer[iter_num] = [copy(sub_df).reset_index(drop=True)]

                output_data = process_part
                process_part = None
                _ = trim_memory()

        if partition_keys is None:
            partition_keys = {p.key for p in self.this_node_complete_iter_to_partitions[iter_num]}

        self.completed_partitions.update(partition_keys)

        self.complete_dataframe(output_data)

    def complete_dataframe(self, df: "pd.DataFrame"):

        if DEBUG:
            print(f"Records to output: {len(df)}")

        batch_max_size: int = self.output_batch_max_size
        batch_on_complete: Callable = self.batch_on_complete
        ordering_columns: list[str] = [PARTITION_KEY_COLUMN] + self.order_by

        with PerformanceLogger("Sorting dataframe"):
            df: "pd.DataFrame" = df.sort_values(
                by=ordering_columns, kind=self.pandas_sort_kind
            ).reset_index(drop=True)

        with PerformanceLogger("Apply row_number"):
            df["row_number"] = df.groupby(by=PARTITION_KEY_COLUMN, sort=False).cumcount().add(1)

        if 0 <= batch_max_size <= len(df):
            with PerformanceLogger("Split to batches"):

                with PerformanceLogger("Calculate output data counts"):
                    partition_keys: pd.Series = df[PARTITION_KEY_COLUMN].value_counts(sort=False)

                current_batch_size = 0
                batches = []
                for pk, count in partition_keys.items():
                    if current_batch_size + count > batch_max_size:
                        batches.append(current_batch_size)
                        current_batch_size = 0
                    current_batch_size += count
                batches.append(current_batch_size)

            with PerformanceLogger("On complete"):
                current_offset = 0
                for batch_size in batches:
                    df_slice = df.iloc[current_offset:current_offset+batch_size, :]
                    batch_on_complete(df=df_slice)
                    current_offset += batch_size
        else:
            batch_on_complete(df)

    def wait_for_shuffle_finish(self, iter_info: "IterationInfo"):
        print_interval_seconds = 10.0
        sleep_time = 0.2
        print_every_n_iter = int(print_interval_seconds / sleep_time)
        counter = 0
        received_records_dict = self.shuffle_received_records

        iter_num = iter_info.iter_num
        input_shuffle_count = iter_info.input_shuffle_count

        while input_shuffle_count > (received_count := received_records_dict[iter_num]) and not self.is_cancelled:
            time.sleep(sleep_time)
            counter += 1
            if counter == print_every_n_iter:
                if DEBUG:
                    print(f"Still waiting for {input_shuffle_count - received_count} records...")
                counter = 0

        if (received_count := received_records_dict[iter_num]) > input_shuffle_count:
            raise ValueError(f"Shuffle excess records: {received_count - input_shuffle_count}")

        print(f"Shuffle for iteration {iter_num} is finished.")
        shuffle_data: list["pd.DataFrame"] = self.shuffle_buffer.pop(iter_num)

        for data in shuffle_data:
            self.put_data(data)

    def run(self):

        start_ts = datetime.now(timezone.utc)
        self.start_time = start_ts

        iterations_info = self.iterations
        complete_iter_to_partitions = self.this_node_complete_iter_to_partitions
        query_id = self.query_id

        for iter_num, iter_info in sorted(iterations_info.items()):

            iter_start_time = datetime.now(timezone.utc)

            block_id = iter_info.block_id

            if DEBUG:
                print(f"Before iteration {iter_num}: {get_rss(True)}")

            if self.is_cancelled:
                break

            # with PerformanceLogger(f"Iteration {iter_num} processed in", ignore_debug=True):

            can_parallel_dump_read_while_shuffle = (
                ((cond := iter_info.sortmerge_partitions_read_to_return_data) is None or len(cond) == 0)
                and not iter_info.use_direct_buffer_return
            )
            should_read_dump = iter_info.read_dump > 0

            if iter_info.write_dump > 0:
                with PerformanceLogger("Write dump"):
                    self.write_dump(iter_info)

            if block_id is not None:
                if DEBUG:
                    print(f"Processing {block_id}")
                with PerformanceLogger("Read and processing block"):
                    self.read_block(iter_info)
                    _ = trim_memory()
            else:
                if DEBUG:
                    print(f"No block to read. Waiting for input data.")

            # We have enough memory to read the dump and receive input shuffle simultaneously.
            if can_parallel_dump_read_while_shuffle and should_read_dump:
                with PerformanceLogger("Naive read dump (while shuffle)"):
                    self.read_dump_naive(iter_info)

            if iter_info.input_shuffle_count > 0:
                with PerformanceLogger("Waiting for input shuffle"):
                    self.wait_for_shuffle_finish(iter_info)

            if should_read_dump and not can_parallel_dump_read_while_shuffle:

                if iter_info.use_direct_buffer_return:
                    keys_to_remove = {
                        p.key
                        for p in complete_iter_to_partitions[iter_num]
                        if len(p.dump_iters) == 0
                    }
                    self.remove_completed(iter_info, partition_keys=keys_to_remove)

                if iter_info.sortmerge_partitions_read_to_return_data is not None:
                    self.read_dump_sortmerge(iter_info)
                else:
                    self.read_dump_naive(iter_info)

            with PerformanceLogger("Remove completed iterations"):
                if iter_info.returned_count > 0:
                    self.remove_completed(iter_info)

            if iter_info.read_dump > 0:
                self.dump_cleanup(iter_info)

            if DEBUG:
                print(f"=====> After iteration {iter_num} (no trim): {get_rss(True)}")

            with PerformanceLogger("trim memory"):
                _ = trim_memory()

            if DEBUG:
                print(f"-**********-> After iteration {iter_num} (after trim): {get_rss(True)}")
                print(f"Sleep time: {self._slept_for_seconds} sec.")

            iter_finish_time = datetime.now(timezone.utc)

            print(
                f"[{query_id}] Iteration {iter_num} processed in: {iter_finish_time - iter_start_time}. "
                f"Details: "
                f"read_records={iter_info.block_size_in_records}, "
                f"input_shuffle={iter_info.input_shuffle_count}, "
                f"output_shuffle={iter_info.output_shuffle_count}, "
                f"dump_read={iter_info.read_dump}, "
                f"dump_write={iter_info.write_dump}, "
                f"returned={iter_info.returned_count}, "
                f"mem_usage={get_rss(True)}, "
                f"current_batches_count={len(self.output_batches)}"
            )

        now = datetime.now(timezone.utc)
        if self.is_cancelled:
            print(f"Query is cancelled. Spent time: {now - start_ts} / Sleep time: {self._slept_for_seconds} sec.")
        else:
            self.is_query_completed = True
            print(f"Query is finished in {now - start_ts} / Sleep time: {self._slept_for_seconds} sec.")
        if DEBUG:
            print(f"After query rss: {get_rss(True)}")

    @PerformanceLoggerDecorator("Extract keys")
    def _extract_keys(self, df: "pd.DataFrame") -> "pd.Series":

        partition_by = self.partition_by

        if len(partition_by) == 1:
            return df[partition_by[0]]

        keys = list(zip(*[df[col] for col in self.partition_by]))
        if len(keys) != len(df):
            raise ValueError(f"Extracted keys len does not match dataframe len: {len(keys)} != {len(df)}")
        return pd.Series(keys)

    def dump_cleanup(self, iter_info: "IterationInfo"):
        current_iter_num = iter_info.iter_num
        iter_dump_dir_path = os.path.join(self.dumps_path, str(current_iter_num))
        shutil.rmtree(iter_dump_dir_path, ignore_errors=True)

    def read_block(self, iter_info: "IterationInfo", *args, **kwargs) -> None:
        block = self.blocks[iter_info.block_id]

        with PerformanceLogger("Fetch data time"):
            data = self.blocks_dataset[block.block_id].read_block(
                block=block, *args, **kwargs
            )

        data[PARTITION_KEY_COLUMN] = self._extract_keys(data)
        partition_nodes = self.partition_nodes
        this_node = self.node_id

        with PerformanceLogger("Set Primary Node"):

            data[PRIMARY_NODE_ID_COLUMN] = data[PARTITION_KEY_COLUMN].apply(lambda x: partition_nodes.get(x))

            if data[PRIMARY_NODE_ID_COLUMN].isnull().any():
                raise ValueError("Null primary node_id found!")

        with PerformanceLogger("Split Dataframe for shuffle"):
            columns = [col for col in data.columns if col not in SYSTEM_COLUMNS]

            shuffle_data: dict[str, "pd.DataFrame"] = dict()

            owned_data: Optional["pd.DataFrame"] = None
            for node_id, df in data.groupby(PRIMARY_NODE_ID_COLUMN, sort=False):
                prep_data = copy(df.drop(columns=PRIMARY_NODE_ID_COLUMN)).reset_index(drop=True)
                if node_id == this_node:
                    owned_data = prep_data
                    continue
                shuffle_data[node_id] = prep_data[columns]

        # Clean up the memory
        data = None
        _ = trim_memory()

        # Shuffle starts here
        iter_num = iter_info.iter_num
        with PerformanceLogger("Submit shuffle tasks"):
            executor = self.executor
            futures = [
                executor.submit(self.output_shuffle, node_id=node_id, df=df, iter_num=iter_num)
                for node_id, df in shuffle_data.items()
            ]

        # While data are shuffling - merge own data
        if owned_data is not None:
            with PerformanceLogger("Merge owned data"):
                self.put_data(owned_data)

        with PerformanceLogger("Shuffle time"):
            for future in concurrent.futures.as_completed(futures):
                _ = future.result()

        _ = trim_memory()

    @PerformanceLoggerDecorator("Serialize data")
    def serialize_data(self,
                       df: "pd.DataFrame",
                       dst: Optional[BinaryIO] = None,
                       chunk_size: Optional[int] = None,
                       ) -> Optional[bytes]:

        columns = [c for c in df.columns if c not in SYSTEM_COLUMNS]
        if len(columns) < len(df.columns):
            df = df[columns]

        if dst is None:
            binary_data = BytesIO()
            df.to_feather(binary_data, chunksize=chunk_size)  # noqa

            # TODO: Check if `.getbuffer` more performant.
            return binary_data.getvalue()

        df.to_feather(dst, chunksize=chunk_size)
        return None

    def write_dump(self, iter_info: "IterationInfo"):

        current_iteration_num = iter_info.iter_num
        partitions_to_dump: set[Hashable] = {
            p.key
            for p in self.this_node_partitions.values()
            if current_iteration_num in p.dump_iters
        }

        if len(partitions_to_dump) == 0:
            raise ValueError(f"Should be dumped {iter_info.write_dump} but actually no keys to dump!")

        if DEBUG:
            print("Writing dump...")

        partitions_complete_iters = self.this_node_partition_to_complete_iter
        data_buffer = self.data_buffer
        partitions_to_dump_iters: set[int] = {partitions_complete_iters[p] for p in partitions_to_dump}

        data_to_dump: dict[int, list[pd.DataFrame]] = dict()
        for iter_num in partitions_to_dump_iters:
            data_to_dump[iter_num] = list()

        with PerformanceLogger("Prepare dump"):
        # TODO: Replace
        # if True:
            call_stack = ".".join([c[3] for c in reversed(inspect.stack()[:5])])

            if DEBUG:
                print(f"Attempting to lock partitions_data_lock {call_stack} ...")

            while self.partitions_data_lock.locked():

                if DEBUG:
                    print("Waiting for lock become released...")

                time.sleep(1)
            self.partitions_data_lock.acquire(blocking=False)

            if DEBUG:
                print(f"Locked partitions_data_lock {call_stack}")

            try:

                for iter_num in partitions_to_dump_iters:

                    dfs: list[pd.DataFrame] = list()

                    partitions_data_list = data_buffer[iter_num]

                    i = 0
                    while i < len(partitions_data_list):
                        df = partitions_data_list[i]
                        if df[PARTITION_KEY_COLUMN].isin(partitions_to_dump).any():
                            dfs.append(partitions_data_list.pop(i))
                        else:
                            i += 1

                    if len(dfs) > 1:
                        processing_df = pd.concat(dfs, ignore_index=True, axis=0, copy=False)
                    elif len(dfs) == 0:
                        print(f"Iter num (partitions_to_dump_iters): {iter_num}")
                        print(f"i counter = {i}")
                        print(f"Partitions should be dumped: "
                              f"{[p for p in partitions_to_dump if partitions_complete_iters[p] == iter_num]}")
                        raise ValueError(f"Data inconsistency. Should be dumped {iter_info.write_dump}, "
                                         f"but there is no data to dump")
                    else:
                        processing_df = dfs[0]

                    dfs = list()
                    processing_df[DUMP_SPLIT_MASK_COLUMN] = processing_df[PARTITION_KEY_COLUMN].isin(partitions_to_dump)

                    with PerformanceLogger("Split dump df to dump partitions"):
                        for is_to_dump, dump_df in processing_df.groupby(DUMP_SPLIT_MASK_COLUMN, sort=False):
                            df_copy = copy(dump_df).drop(columns=[DUMP_SPLIT_MASK_COLUMN])
                            if is_to_dump:
                                data_to_dump[iter_num].append(df_copy)
                            else:
                                partitions_data_list.append(
                                    df_copy.reset_index(drop=True)
                                )

                    processing_df = None
                    _ = trim_memory()

            finally:
                if DEBUG:
                    print(f"Releasing partitions_data_lock {call_stack} ...")
                self.partitions_data_lock.release()
                if DEBUG:
                    print(f"Released partitions_data_lock {call_stack}")

        with PerformanceLogger("Write dump to disk"):

            executor = self.executor
            futures = list()
            total_write_cnt = 0

            for complete_iter_num, dfs_list in data_to_dump.items():
                
                if len(dfs_list) > 1:
                    df = pd.concat(dfs_list, ignore_index=True, copy=False)
                else:
                    df = dfs_list[0]

                total_write_cnt += len(df)

                if self.parallel_dumping:
                    futures.append(
                        executor.submit(
                            self._write_df_to_dump,
                            df=df,
                            complete_iter_num=complete_iter_num,
                            current_iter_num=current_iteration_num,
                        )
                    )
                else:
                    self._write_df_to_dump(
                        df=df,
                        complete_iter_num=complete_iter_num,
                        current_iter_num=current_iteration_num,
                    )

            if self.parallel_dumping:
                # TODO: Do not wait for dump is finished
                for future in concurrent.futures.as_completed(futures):
                    _ = future.result()

            if total_write_cnt != iter_info.write_dump:
                raise ValueError(
                    f"Expected write to dump: {iter_info.write_dump}. "
                    f"Actually wrote: {total_write_cnt}"
                )

        data_to_dump = dict()
        _ = trim_memory()

    def _write_df_to_dump(self, df: "pd.DataFrame", current_iter_num: int, complete_iter_num: int) -> None:

        if len(df) > 0:

            is_sortmerge = self.iterations[complete_iter_num].sortmerge_partitions_read_to_return_data is not None
            batch_size: Optional[int] = self.sortmerge_batch_size if is_sortmerge else None

            if is_sortmerge:
                df = df.sort_values(by=[PARTITION_KEY_COLUMN], kind=self.pandas_sort_kind, ignore_index=True)

            with PerformanceLogger(f"Write dump batch {current_iter_num}->{complete_iter_num}"):

                dir_path = os.path.join(self.dumps_path, str(complete_iter_num))

                os.makedirs(dir_path, exist_ok=True)

                filepath = os.path.join(dir_path, f"{current_iter_num}.feather")

                # TODO: Compare performance
                # data_bytes: bytes = self.serialize_data(df.reset_index(drop=True), chunk_size=batch_size)
                # os.system(f"fallocate -l {len(data_bytes)} {filepath}")
                # with open(filepath, "wb") as f:
                #     f.write(data_bytes)

                with open(filepath, "wb") as f:
                    self.serialize_data(df.reset_index(drop=True), dst=f, chunk_size=batch_size)

                _ = trim_memory()

                if DEBUG:
                    print(f"Wrote {filepath}")

        else:
            print(f"WARNING! {current_iter_num}->{complete_iter_num} df is empty!")

    def _read_df_from_dump(self, path: str, iter_num: int) -> "pd.DataFrame":
        with PerformanceLogger(f"Read dump chunk {path}"):

            try:
                df = pd.read_feather(path)
            except pa.lib.ArrowInvalid:
                print(f"WARNING! Unable to read feather file: {path}")
                print(os.system(f"ls -lah {os.path.join(self.dumps_path, str(iter_num))}"))
                print("Try again...")
                time.sleep(5)
                df = pd.read_feather(path)

        return df

    def read_dump_naive(self, iter_info: "IterationInfo"):

        _ = trim_memory()

        current_iter_num = iter_info.iter_num

        iter_dump_dir_path = os.path.join(self.dumps_path, str(current_iter_num))

        dfs: list[pd.DataFrame] = list()

        for filename in os.listdir(iter_dump_dir_path):
            if filename.endswith(".feather"):
                filepath = os.path.join(iter_dump_dir_path, filename)

                dump_part_df = self._read_df_from_dump(path=filepath, iter_num=current_iter_num)
                dfs.append(dump_part_df)
                _ = trim_memory()

        if len(dfs) > 1:
            df = pd.concat(dfs, ignore_index=True, axis=0)
        else:
            df = dfs[0]

        dfs = list()
        _ = trim_memory()

        self.put_data(df)

    @staticmethod
    def _open_dump_file_reader(path: str) -> "RecordBatchFileReader":
        return ipc.open_file(path)

    def read_dump_sortmerge(self, iter_info: "IterationInfo"):

        current_iter_num = iter_info.iter_num
        completed_partitions = self.completed_partitions
        dumps_path = self.dumps_path
        batch_size = self.sortmerge_batch_size

        partitions_count_to_remove_completed: Union[list[int], "np.ndarray[int]"] = copy(
            iter_info.sortmerge_partitions_read_to_return_data
        )

        if isinstance(partitions_count_to_remove_completed, np.ndarray):
            partitions_count_to_remove_completed = partitions_count_to_remove_completed.tolist()

        partitions_order: list["Partition"] = sorted([
            p
            for p in self.this_node_complete_iter_to_partitions[current_iter_num]
            if p.key not in completed_partitions
        ], key=lambda x: x.key)

        # src_iter_num = Iteration when data were dumped
        # {src_iter_num -> (reader, current_batch)}
        readers: dict[int, "RecordBatchFileReader"] = dict()

        # {src_iter_num -> read_records_count}
        readers_read_records_count: dict[int, int] = dict()

        # {src_iter_num -> used_records_count}
        readers_used_records_count: dict[int, int] = dict()

        dump_data: list["pd.DataFrame"] = list()

        read_partitions_counter = 0
        next_remove_completed: Optional[int] = (
            partitions_count_to_remove_completed.pop(0)
            if len(partitions_count_to_remove_completed) > 0
            else None
        )

        keys_to_remove: set[Hashable] = set()

        for partition in partitions_order:

            for iter_num, count in partition.dump_iters.items():

                if iter_num not in readers:
                    file_path = os.path.join(dumps_path, str(current_iter_num), f"{iter_num}.feather")
                    reader = self._open_dump_file_reader(file_path)
                    readers[iter_num] = reader
                    readers_read_records_count[iter_num] = 0
                    readers_used_records_count[iter_num] = 0
                else:
                    reader = readers[iter_num]

                already_read_records = readers_read_records_count[iter_num]
                already_used_records = readers_used_records_count[iter_num]

                if already_used_records + count > already_read_records:
                    batch_to_read = int(already_read_records / batch_size)
                    batch_df: "pd.DataFrame" = reader.get_batch(batch_to_read).to_pandas()
                    readers_read_records_count[iter_num] += len(batch_df)
                    dump_data.append(batch_df)

                readers_used_records_count[iter_num] += count

            read_partitions_counter += 1
            keys_to_remove.add(partition.key)

            if next_remove_completed is not None and read_partitions_counter == next_remove_completed:

                self.put_data(pd.concat(dump_data, ignore_index=True, axis=0))
                dump_data = list()

                self.remove_completed(iter_info, partition_keys=keys_to_remove)
                keys_to_remove = set()

                next_remove_completed = (
                    partitions_count_to_remove_completed.pop(0)
                    if len(partitions_count_to_remove_completed) > 0
                    else None
                )
                _ = trim_memory()

        if len(dump_data) > 0:
            self.put_data(pd.concat(dump_data, ignore_index=True, axis=0))
            dump_data = list()
            # The rest data will be returned in the `run` method.
            # self.remove_completed(iter_info, partition_keys=keys_to_remove)
            keys_to_remove = set()

    def output_shuffle(self, node_id: str, iter_num: int, df: "pd.DataFrame"):

        if DEBUG:
            print(f"Shuffle to {node_id} - {len(df)}")

        # TODO: Replace address and validate URL
        node_status = discovery_service.validate_node(node_id)
        url = f"{node_status.address}/worker/query/{self.query_id}/shuffle/{iter_num}/"

        with BytesIO() as data_file:

            with PerformanceLogger("Serialize shuffle data"):
                self.serialize_data(df, data_file)
                data_file.seek(0)

            with PerformanceLogger(f"Send data to node ({node_id})"):
                try:
                    response = requests.post(
                        url=url,
                        files={
                            "data": data_file,
                            # "data": ("shuffle_data", data_file)
                            # "data": data_file.getvalue(),
                        }
                    )
                    size = response.request.headers['Content-length']

                    if response.status_code != 200:
                        raise ValueError(f"Response({response.status_code}) - {response.content}")

                except Exception as e:
                    raise e
                else:
                    print(f"Content-length (to {node_id}): {size}")

    def input_shuffle(self, iter_num: int, data: BytesIO):

        with PerformanceLogger("Input shuffle"):

            data.seek(0)

            df: "pd.DataFrame" = pd.read_feather(data)
            self.shuffle_buffer[iter_num].append(df)
            self.shuffle_received_records[iter_num] += len(df)

    def put_data(self, df: pd.DataFrame):

        partitions_completed_iters = self.this_node_partition_to_complete_iter

        if DEBUG:
            print("Merging data...")
        with PerformanceLogger("Merge data"):
            with PerformanceLogger("Assign partition key and completed iteration"):
                if PARTITION_KEY_COLUMN not in df.columns:
                    df[PARTITION_KEY_COLUMN] = self._extract_keys(df)

                df[COMPLETE_ITER_COLUMN] = df[PARTITION_KEY_COLUMN].apply(lambda x: partitions_completed_iters.get(x))

            with PerformanceLogger("Add partition data to node buffer"):
                iter_num: int
                call_stack = ".".join([c[3] for c in reversed(inspect.stack()[:5])])
                if DEBUG:
                    print(f"Attempting to lock partitions_data_lock {call_stack} ...")
                while self.partitions_data_lock.locked():
                    if DEBUG:
                        print("Waiting for lock become released...")
                    time.sleep(1)
                self.partitions_data_lock.acquire(blocking=False)
                if DEBUG:
                    print(f"Locked partitions_data_lock {call_stack}")
                try:
                    data_buffer = self.data_buffer
                    for iter_num, df in df.groupby(COMPLETE_ITER_COLUMN, sort=False):
                        df_copy = copy(df)
                        try:
                            data_buffer[iter_num].append(df_copy)
                        except KeyError:
                            data_buffer[iter_num] = [df_copy]
                finally:
                    if DEBUG:
                        print(f"Releasing partitions_data_lock {call_stack} ...")
                    self.partitions_data_lock.release()
                    if DEBUG:
                        print(f"Released partitions_data_lock {call_stack}")
