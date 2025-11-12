import json
import time
from typing import Hashable, Optional, Iterator, TYPE_CHECKING, Generator
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from operator import itemgetter
import requests
from io import BytesIO

from requests import JSONDecodeError

from parwop.utils import PerformanceLogger, trim_memory, PerformanceLoggerDecorator
from parwop.settings import NODE_ID
from parwop.server.discovery import discovery_service
from parwop.server.mq import mq_service
from parwop.solver.partition import Partition
from parwop.solver.solver import Solver
from parwop.worker.worker import Worker


if TYPE_CHECKING:
    from parwop.solver.block import Block
    from parwop.solver.dataset import Dataset
    from parwop.server.discovery import NodeStatus
    from concurrent.futures import Future


class QueryDriver:

    def __init__(self, *,
                 datasets: list["Dataset"],
                 partition_by: list[str],
                 order_by: list[str],
                 is_distributed: bool = False,
                 query_id: Optional[str] = None,
                 memory_limit: Optional[int] = None,
                 worker_kwargs: Optional[dict] = None,
                 solver_kwargs: Optional[dict] = None,):

        dataset_ids: list[str] = [ds.dataset_id for ds in datasets]
        unique_ids = set(dataset_ids)
        if len(datasets) != len(unique_ids):
            raise ValueError(
                f"Dataset IDs should be unique! Passed IDs: {dataset_ids}. Unique: {unique_ids}"
            )

        if not is_distributed:
            for ds in datasets:
                if ds.node_id is None:
                    ds.node_id = NODE_ID
        elif any([ds.node_id is None for ds in datasets]):
            raise ValueError(
                f"Datasets' node assignment is optional only in non-distributed mode! "
                f"At least one dataset haven't bind to node!"
            )

        self.datasets: list["Dataset"] = datasets
        self.partition_by: list[str] = partition_by
        self.order_by: list[str] = order_by
        self.is_distributed: bool = is_distributed
        self.query_id: str = query_id if query_id is not None else str(uuid4())
        self.error: bool = False

        self.partitions: dict[Hashable, "Partition"] = dict()
        self.blocks: list["Block"] = list()
        self.nodes: set[str] = {ds.node_id for ds in datasets}

        self.is_plans_built: bool = False
        self.solver: Optional["Solver"] = None
        self.memory_limit: Optional[int] = memory_limit
        self.worker_kwargs: dict = worker_kwargs if worker_kwargs is not None else dict()
        self.solver_kwargs: dict = solver_kwargs if solver_kwargs is not None else dict()
        self._local_worker: Optional["Worker"] = None

    @property
    def local_worker(self) -> "Worker":
        if self.is_distributed:
            raise ValueError(f"Local worker is not allowed in distributed mode")
        return self._local_worker

    @PerformanceLoggerDecorator("Build Plans")
    def build_plans(self):

        blocks: list["Block"] = self.collect_block_statistics()
        self.blocks = blocks
        total_records = 0
        for block in self.blocks:
            total_records += block.size_in_records

        print(f"Total records: {total_records}")

        self.nodes = {b.node_id for b in blocks}

        partitions: dict[Hashable, "Partition"] = self.construct_partitions(blocks)
        print("Construct partitions")
        self.partitions = partitions

        self.assign_partitions_to_nodes(partitions)
        print("Assign partitions to nodes")

        solver = Solver(
            partition_by=self.partition_by,
            blocks=self.blocks,
            partitions=self.partitions,
            **self.solver_kwargs
        )

        self.solver = solver

        with PerformanceLogger("Determine blocks sequence"):
            solver.determine_blocks_sequence()

        with PerformanceLogger("Build dump plans"):
            solver.prepare_dumps(memory_limit=self.memory_limit)

        self.is_plans_built = True

    @PerformanceLoggerDecorator(message="Construct Partitions")
    def construct_partitions(self, blocks: list["Block"]):
        nodes = {b.node_id for b in blocks}
        all_statistics = [b.statistic for b in blocks]

        if len(nodes) > 1:
            primary_node = None
        else:
            primary_node = list(nodes)[0]

        tmp_df = self.merge_statistics(all_statistics)

        all_statistics = None
        trim_memory()

        partitions: dict[Hashable, "Partition"] = dict()
        for key, count in tmp_df.iloc[:, 0].items():
            partitions[key] = Partition(
                key=key,
                primary_node=primary_node,
                records_count=count,
            )

        return partitions

    @PerformanceLoggerDecorator("Collect Block Statistics")
    def collect_block_statistics(self) -> list["Block"]:

        if self.is_distributed:
            blocks: list["Block"] = self.collect_block_statistics_distributed()
        else:
            blocks: list["Block"] = self.collect_block_statistics_local()

        if len(blocks) < 1:
            raise ValueError(f"No blocks found for specified datasets: {self.datasets}")

        return blocks

    def merge_statistics(self, all_statistics: list["pd.Series[int]"]) -> "pd.DataFrame":
        # Returns pd.DataFrame with the following structure: Index[key], count:int
        return pd.DataFrame(
            pd.concat(all_statistics, sort=False, ignore_index=False)
        # ).groupby(level=self.partition_by, sort=False).agg(count=(0, "sum"))
        ).groupby(level="key", sort=False).agg(count=("count", "sum"))

    @PerformanceLoggerDecorator("Assign Partitions to Nodes")
    def assign_partitions_to_nodes(self, partitions: dict[Hashable, "Partition"]) -> None:
        blocks = self.blocks
        all_nodes: set[str] = self.nodes

        if len(all_nodes) == 1:
            primary_node = list(all_nodes)[0]
            for p in partitions.values():
                p.primary_node = primary_node
            return

        stats: dict[str, list["pd.Series[int]"]] = dict()
        for b in blocks:
            b_stats = b.statistic
            try:
                stats[b.node_id].append(b_stats)
            except KeyError:
                stats[b.node_id] = [b_stats]

        with PerformanceLogger("Total stats merge"):
            total_stats = self.merge_statistics([b.statistic for b in blocks]).rename(columns={"count": "total_count"})

        with PerformanceLogger("Nodes calc"):
            sort_columns = ["weight", "key"] # + self.partition_by
            ascending_rule = [False, True] # + [True] * len(self.partition_by)
            node_weights: dict[str, Iterator[tuple[Hashable, int]]] = dict()
            for node in all_nodes:
                node_df: "pd.DataFrame" = pd.concat(
                    [
                        self.merge_statistics(stats.pop(node)),
                        total_stats
                    ], sort=False, axis=1, join="inner")
                node_df["weight"] = node_df["count"] / node_df["total_count"]
                node_weights[node] = node_df.sort_values(by=sort_columns, ascending=ascending_rule)["weight"].items()

        node_records_count: dict[str, int] = {n: 0 for n in all_nodes}
        final_node_records_count: dict[str, int] = dict()

        assigned_partitions: set[Hashable] = set()
        assigned_partitions_cnt = 0

        total_partitions_cnt = len(partitions)

        with PerformanceLogger("Assign partitions while"):
            value_getter = itemgetter(1)
            while assigned_partitions_cnt < total_partitions_cnt:

                # This approach has ~20% better performance than the following:
                # node_id, count = min(node_records_count.items(), key=lambda item: item[1])
                node_id, count = min(node_records_count.items(), key=value_getter)  # ~100ms / 1M iters
                current_iterator = node_weights[node_id]                            # ~50 ms / 1M iters
                p = None

                for key, weight in current_iterator:
                    if key in assigned_partitions:
                        continue
                    p = partitions[key]
                    break

                if p is None:
                    # We have checked all the node partitions and there are no partitions.
                    # Remove this node from processing.
                    final_node_records_count[node_id] = node_records_count.pop(node_id)
                    _ = node_weights.pop(node_id)
                    continue

                p.primary_node = node_id
                node_records_count[node_id] += p.initial_size

                assigned_partitions.add(p.key)
                assigned_partitions_cnt += 1                                        # ~70 ms / 1M iters

            for node_id, count in node_records_count.items():
                final_node_records_count[node_id] = count

        print(f"Nodes balanced: {final_node_records_count}. Total partitions count: {len(partitions)}")

    def clear_driver_metadata(self):
        # Remove all metadata: blocks meta, partitions, etc to free memory.
        self.solver = None
        self.partitions = dict()
        self.blocks = list()
        trim_memory()

    def get_metadata(self) -> tuple["pd.DataFrame", "pd.DataFrame", "pd.DataFrame", dict[str, "pd.DataFrame"]]:

        if not self.is_plans_built:
            self.build_plans()

        return (
            self.get_datasets_metadata(),
            self.get_blocks_metadata(),
            self.get_partitions_metadata(),
            self.get_node_buffers_metadata(),
        )

    def run_distributed(self) -> None:
        # Do nothing, lifecycle is controlled by coordinator_query_service
        return

    def run_local(self) -> None:

        datasets_metadata, blocks_metadata, partitions_metadata, node_iterations_metadata = self.get_metadata()

        try:

            worker = Worker(
                node_id=NODE_ID,
                partition_by=self.partition_by,
                order_by=self.order_by,
                query_id=str(uuid4()),
                datasets_metadata=datasets_metadata,
                blocks_metadata=blocks_metadata,
                partitions_metadata=partitions_metadata,
                node_iterations_metadata=node_iterations_metadata[NODE_ID],
                is_local=True,
                **self.worker_kwargs,
            )

            self._local_worker = worker

            # Block until finish
            worker.run()

        except Exception as e:
            self.error = True
            raise e

    def run(self) -> None:
        if self.is_distributed:
            return self.run_distributed()
        else:
            return self.run_local()

    def collect_block_statistics_local(self) -> list["Block"]:
        return Worker.collect_block_statistics(partition_by=self.partition_by, datasets=self.datasets)

    @staticmethod
    def _request_statistics_from_worker(node_id: str,
                                        query_id: str,
                                        datasets: list["Dataset"],
                                        partition_by: list[str]) -> list["Block"]:

        node: Optional["NodeStatus"] = discovery_service.nodes.get(node_id, None)

        if node is None:
            raise ValueError(f"Node {node_id} not known")

        node_address = node.address

        # TODO: Make async?
        #  Pros - no alive connection while statistics is calculating
        #  Cons - Will not work if driver is not a server
        response = requests.post(
            f"{node_address}/worker/statistic/",
            json={
                "partition_by": partition_by,
                "datasets": [
                    {
                        "dataset_type": ds.dataset_type,
                        "dataset_kwargs": {
                            "path": ds.path,
                            "dataset_id": ds.dataset_id,
                            "node_id": ds.node_id,
                        }
                    }
                    for ds in datasets
                ],
            },
        )

        if response.status_code != 200:
            raise ValueError(
                f"Request {node_address}/{query_id} failed. "
                f"Status Code: {response.status_code}. "
                f"Content: {response.content}"
            )

        blocks_df = pd.read_parquet(BytesIO(response.content))

        return list(Worker.load_blocks_metadata(blocks_df, with_statistics=True).values())

    def collect_block_statistics_distributed(self) -> list["Block"]:

        node_datasets = {node_id: list() for node_id in self.nodes}

        for ds in self.datasets:
            node_datasets[ds.node_id].append(ds)

        futures: list["Future"] = list()
        with ThreadPoolExecutor(max_workers=10) as executor:
            for node_id in self.nodes:
                futures.append(
                    executor.submit(
                        self._request_statistics_from_worker,
                        node_id=node_id,
                        query_id=self.query_id,
                        datasets=node_datasets[node_id],
                        partition_by=self.partition_by,
                    )
                )

        result: list["Block"] = list()
        for future in futures:
            result.extend(future.result())

        return result

    @PerformanceLoggerDecorator("Get partitions metadata")
    def get_partitions_metadata(self) -> "pd.DataFrame":
        return pd.DataFrame(
            [p.to_dict() for p in self.partitions.values()]
        )

    @PerformanceLoggerDecorator("Get blocks metadata")
    def get_blocks_metadata(self) -> "pd.DataFrame":
        return pd.DataFrame(
            [b.to_dict(with_statistic=False) for b in self.blocks]
        )

    @PerformanceLoggerDecorator("Get datasets metadata")
    def get_datasets_metadata(self) -> "pd.DataFrame":
        return pd.DataFrame(
            [
                ds.to_dict(with_blocks=False, with_block_statistics=False)
                for ds in self.datasets
            ]
        )

    @PerformanceLoggerDecorator("Get Blocks Sequence Metadata")
    def get_sequence_metadata(self) -> "pd.DataFrame":
        return self.solver.sequence.get_block_sequence_metadata(debug_info=False)

    @PerformanceLoggerDecorator("Get Node Buffers Metadata")
    def get_node_buffers_metadata(self) -> dict[str, "pd.DataFrame"]:
        return {
            node_id: buffer.get_metadata()
            for node_id, buffer in self.solver.node_buffers.items()
        }


class QueryClient:

    def __init__(self, *,
                 datasets: list["Dataset"],
                 partition_by: list[str],
                 order_by: list[str],
                 is_distributed: bool = False,
                 coordinator_address: Optional[str] = None,
                 memory_limit: Optional[int] = None,
                 worker_kwargs: Optional[dict] = None,
                 solver_kwargs: Optional[dict] = None,):

        if coordinator_address:
            is_distributed = True

        self.query_id = str(uuid4())
        self.datasets = datasets
        self.partition_by = partition_by
        self.order_by = order_by
        self.is_distributed = is_distributed
        self.coordinator_address = coordinator_address
        self.memory_limit = memory_limit
        self.worker_kwargs = worker_kwargs
        self.solver_kwargs = solver_kwargs

    def _run_local(self) -> Generator["pd.DataFrame", None, None]:

        driver = QueryDriver(
            datasets=self.datasets,
            partition_by=self.partition_by,
            order_by=self.order_by,
            is_distributed=False,
            query_id=self.query_id,
            memory_limit=self.memory_limit,
            worker_kwargs=self.worker_kwargs,
            solver_kwargs=self.solver_kwargs,
        )

        with ThreadPoolExecutor(max_workers=1) as executor:
            driver_future: "Future[None]" = executor.submit(driver.run)

            while driver.local_worker is None:
                time.sleep(0.1)
                if driver.error:
                    driver_future.result()
                    raise ValueError()

            worker = driver.local_worker

            current_batch_num = 1

            try:
                while True:

                    current_completed_batches = list(worker.output_batches.keys())
                    if len(current_completed_batches) == 0:

                        if driver_future.done():
                            break

                        time.sleep(0.1)
                        continue

                    for batch_num in current_completed_batches:
                        batch_df = worker.get_data_batch(batch_num)
                        current_batch_num += 1
                        yield batch_df

            except Exception as e:
                print(f"Error: {e}. Batch: {current_batch_num}")
                worker.is_cancelled = True
                driver_future.cancel()
                raise e

            else:
                _ = driver_future.result()

    def _run_distributed(self) -> Generator["pd.DataFrame", None, None]:
        response = requests.post(
            url=f"{self.coordinator_address}/coordinator/query/{self.query_id}/",
            json={
                "partition_by": self.partition_by,
                "order_by": self.order_by,
                "datasets": [
                    {
                        "dataset_type": ds.dataset_type,
                        "dataset_kwargs": {
                            "dataset_id": ds.dataset_id,
                            "node_id": ds.node_id,
                            "path": ds.path,
                        }
                    }
                    for ds in self.datasets
                ],
                "memory_limit_records": self.memory_limit,
                "worker_kwargs": self.worker_kwargs,
                "solver_kwargs": self.solver_kwargs,
            }
        )

        if response.status_code != 200:
            raise ValueError(
                f"Unable to run query {self.query_id}: Status code {response.status_code}. "
                f"Details: {response.content}"
            )

        return self.consume_mq()

    def consume_mq(self) -> Generator["pd.DataFrame", None, None]:

        response = requests.get(
            url=f"{self.coordinator_address}/coordinator/heartbeat/status/"
        )

        if response.status_code != 200:
            try:
                detail = response.json()
            except JSONDecodeError:
                detail = response.content
            print(f"WARNING! Heartbeat failed. Reason: {detail}")
        else:
            data = response.json()
            discovery_service.update_rabbitmq(host=data["rabbitmq"]["host"], port=data["rabbitmq"]["port"])

        channel, connection = mq_service.get_channel(extra_connection_params={"heartbeat": 0})

        if channel is None:
            raise ValueError("Unable to get channel. Probably, the rabbitmq is not set.")

        topic = self.query_id
        channel.queue_declare(queue=topic)

        # TODO: Add query cancelling on KeyboardInterrupt
        while True:
            method_frame, _, body = channel.basic_get(topic, auto_ack=False)

            stop_reason: Optional[str] = None
            if method_frame:
                message = json.loads(body.decode("utf-8"))

                for key in ("error", "timeout", "cancelled", "finished"):
                    if message.get(key, None):
                        stop_reason = key

                        if key == "error":
                            print(f"Error occurred during query execution:")
                            print(message["error"])

                if stop_reason:
                    channel.basic_nack(method_frame.delivery_tag)
                    channel.close()
                    connection.close()

                    if stop_reason in ("error", "timeout", "cancelled"):
                        raise ValueError(f"Query state has been changed to state: {stop_reason}")

                    return

                response = requests.get(message["address"])
                if response.status_code != 200:
                    channel.basic_nack(method_frame.delivery_tag)
                    raise ValueError(f"Unable to get data batch: {response.content}")

                channel.basic_ack(method_frame.delivery_tag)

                yield pd.read_feather(BytesIO(response.content))

    def run(self) -> Generator["pd.DataFrame", None, None]:
        print(f"Running query {self.query_id}")
        if self.is_distributed:
            return self._run_distributed()
        else:
            return self._run_local()
