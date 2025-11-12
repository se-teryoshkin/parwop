from typing import Hashable, TYPE_CHECKING

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import numpy as np

if TYPE_CHECKING:
    from parwop.solver.block import Block
    from parwop.solver.partition import Partition


class IterMetrics:

    __slots__ = ("iter_num", "block_id", "partitions_completed_count",
                 "records_completed_count", "buffer_records_count")

    def __init__(self, *,
                 iter_num: int,
                 block_id: str,
                 partitions_completed_count: int,
                 records_completed_count: int,
                 buffer_records_count: int,
                 ):

        self.iter_num = iter_num
        self.block_id = block_id
        self.partitions_completed_count = partitions_completed_count
        self.records_completed_count = records_completed_count
        self.buffer_records_count = buffer_records_count


class BlockSequence:

    def __init__(self):

        # On each iteration we have multiple parallel nodes reading.
        # That's why we have a list of blocks for each iteration
        self.blocks_sequence: dict[int, list["Block"]] = dict()  # iter_num -> list[Block]
        self.iteration_metrics: dict[int, list["IterMetrics"]] = dict()
        self.completed_partitions: dict[int, list[list[Hashable]]] = dict()

    def add_block(self, *,
                  block: "Block",
                  iter_num: int,
                  completed_partitions: list["Partition"],
                  buffer_records_count: int):

        iter_metrics = IterMetrics(
            iter_num=block.read_on_iteration,
            block_id=block.block_id,
            partitions_completed_count=len(completed_partitions),
            records_completed_count=sum([p.initial_size for p in completed_partitions]),
            buffer_records_count=buffer_records_count,
        )

        completed_partitions_keys = [p.key for p in completed_partitions]

        try:
            self.blocks_sequence[iter_num].append(block)
            self.iteration_metrics[iter_num].append(iter_metrics)
            self.completed_partitions[iter_num].append(completed_partitions_keys)
        except KeyError:
            self.blocks_sequence[iter_num] = [block]
            self.iteration_metrics[iter_num] = [iter_metrics]
            self.completed_partitions[iter_num] = [completed_partitions_keys]

    def __repr__(self):
        return (
            f"GlobalBlocksPlan(iters={[f'{k} -> {[b.block_id for b in v]}' for k, v in self.blocks_sequence.items()]})"
        )

    def get_block_sequence_metadata(self, debug_info: bool = False):

        data = list()
        metrics = self.iteration_metrics
        completed_partitions = self.completed_partitions

        for iter_num, blocks in self.blocks_sequence.items():
            metrics_iter_num = metrics[iter_num]
            completed_partitions_iter_num = completed_partitions[iter_num]
            for i, block in enumerate(blocks):

                info = {"iter_num": iter_num, "block_id": block.block_id}

                if debug_info:
                    info.update({  # noqa
                        "block_score": block.score,
                        "completed_partitions": completed_partitions_iter_num[i],
                        "metrics": metrics_iter_num[i]
                    })

                data.append(info)

        return pd.DataFrame(data)

    @staticmethod
    def load_from_file(path: str, blocks_dict: dict[str, "Block"]) -> "BlockSequence":

        # TODO: Check

        t = pq.read_table(path)

        metadata = {
            k.decode("utf-8"): v.decode("utf-8")
            for k, v in t.schema.metadata.items()
        }

        df = t.to_pandas()

        blocks_sequence = dict()
        metrics = dict()
        completed_partition_keys = dict()

        partition_by = metadata["partition_by"]
        try:
            partition_by = json.loads(partition_by)
        except:
            partition_by = [partition_by]

        plan = BlockSequence()

        _array_types_tuple = (list, np.ndarray)

        for iter_num in df["iter_num"].unique():
            blocks_sequence[iter_num] = list()
            metrics[iter_num] = list()
            completed_partition_keys[iter_num] = list()

        for _, row in df.iterrows():
            iter_num = row["iter_num"]
            row_metrics = row["metrics"]
            completed_partitions = row["completed_partitions"]

            # TODO: Perform refactoring
            if len(completed_partitions) > 0:
                key_example = completed_partitions[0]
                if isinstance(key_example, _array_types_tuple):
                    completed_partitions = [
                        tuple(v) for v in completed_partitions
                    ]
                else:
                    completed_partitions = [
                        tuple([v]) for v in completed_partitions
                    ]

            metrics[iter_num].append(row_metrics)
            blocks_sequence[iter_num].append(blocks_dict[row["block_id"]])
            completed_partition_keys[iter_num].append(completed_partitions)

        plan.iteration_metrics = metrics
        plan.blocks_sequence = blocks_sequence
        plan.completed_partitions = completed_partition_keys

        return plan
