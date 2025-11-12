from datetime import datetime, timezone
from typing import Hashable, Optional, TYPE_CHECKING, Generator
import pandas as pd
from random import choice

from parwop.solver.plan import BlockSequence
from parwop.solver.buffer import NodeBuffer, GlobalBuffer
from parwop.utils import PerformanceLogger, trim_memory

if TYPE_CHECKING:
    from parwop.solver.block import Block  # noqa
    from parwop.solver.partition import Partition  # noqa


class Solver:

    __slots__ = (
        "global_buffer", "sequence", "node_buffers",
        "partitions", "blocks", "partition_by",
        "blocks_dict", "nodes", "func_name", "_sequence_func"
    )

    def __init__(self, *,
                 partition_by: list[str],
                 blocks: list["Block"],
                 partitions: dict[Hashable, "Partition"],
                 func_name: str = "subsets_heuristic",
                 ):

        self.global_buffer = GlobalBuffer(partitions=partitions)
        self.sequence: Optional["BlockSequence"] = None
        self.node_buffers: dict[str, "NodeBuffer"] = dict()  # node_id -> NodeBuffer

        self.partitions = partitions
        self.blocks = blocks
        self.blocks_dict: dict[str, "Block"] = {b.block_id: b for b in blocks}
        self.nodes: set[str] = {b.node_id for b in blocks}
        self.partition_by = partition_by
        self.func_name = func_name
        self._sequence_func = {
            "subsets_heuristic": self._determine_blocks_sequence_subsets_heuristic,
            "random": self._determine_blocks_sequence_random,
            "sequential": self._determine_blocks_sequence_sequential,
        }[func_name]

    def determine_blocks_sequence(self):

        processed_blocks = 0
        blocks_count = len(self.blocks)

        buffer = self.global_buffer

        blocks_sequence = BlockSequence()

        for best_block, best_score, iter_num in self._sequence_func():

            iter_start_ts = datetime.now(timezone.utc)

            completed_partitions, _ = buffer.load_to_buffer(block=best_block)

            total_returned_records = 0
            for partition in completed_partitions:
                partition.completed(iter_num)
                total_returned_records += partition.initial_size

            best_block.mark_as_read(iter_num=iter_num, score=best_score)

            blocks_sequence.add_block(
                block=best_block,
                iter_num=iter_num,
                completed_partitions=completed_partitions,
                buffer_records_count=buffer.buffered_size
            )

            print(f"--- {iter_num} ({best_block.node_id}) --- (elapsed: {datetime.now(timezone.utc) - iter_start_ts})")
            print(f"Best block={best_block} (returned = {total_returned_records})")
            print(buffer)

            processed_blocks += 1

        if processed_blocks != blocks_count:
            raise ValueError(f"Processed blocks {processed_blocks} != blocks count {blocks_count}!")

        self.sequence = blocks_sequence

    def _determine_blocks_sequence_sequential(self) -> Generator[tuple["Block", float, int], None, None]:
        blocks = self.blocks

        total_blocks = len(blocks)
        iter_num = 0
        processed_blocks = 0
        processed_blocks_ids = set()
        used_nodes: set[str] = set()
        available_nodes = self.nodes.copy()

        while processed_blocks < total_blocks:
            candidates = [
                blk
                for blk in blocks
                if blk.node_id not in used_nodes and blk.block_id not in processed_blocks_ids
            ]

            if len(candidates) == 0:
                raise ValueError("No blocks")

            block = candidates[0]
            block_id = block.block_id

            yield block, 1.0, iter_num

            processed_blocks += 1
            processed_blocks_ids.add(block_id)
            used_nodes.add(block.node_id)

            if len(used_nodes) == len(available_nodes):
                used_nodes: set[str] = set()
                available_nodes = {blk.node_id for blk in blocks if not blk.is_read}
                iter_num += 1

    def _determine_blocks_sequence_random(self) -> Generator[tuple["Block", float, int], None, None]:
        blocks = self.blocks

        total_blocks = len(blocks)
        iter_num = 0
        processed_blocks = 0
        processed_blocks_ids = set()
        used_nodes: set[str] = set()
        available_nodes = self.nodes.copy()

        while processed_blocks < total_blocks:
            candidates = [
                blk
                for blk in blocks
                if blk.node_id not in used_nodes and blk.block_id not in processed_blocks_ids
            ]

            if len(candidates) == 0:
                raise ValueError("No blocks")

            block = choice(candidates)
            block_id = block.block_id

            yield block, 1.0, iter_num

            processed_blocks += 1
            processed_blocks_ids.add(block_id)
            used_nodes.add(block.node_id)

            if len(used_nodes) == len(available_nodes):
                used_nodes: set[str] = set()
                available_nodes = {blk.node_id for blk in blocks if not blk.is_read}
                iter_num += 1

    def _determine_blocks_sequence_subsets_heuristic(self) -> Generator[tuple["Block", float, int], None, None]:
        blocks = self.blocks
        blocks_dict = self.blocks_dict

        blocks_in_partition: dict[Hashable, set[str]] = dict()
        with PerformanceLogger("Prepare partitions dict"):
            for block_id, blk in blocks_dict.items():
                for key in blk.statistic.keys():
                    try:
                        blocks_in_partition[key].add(block_id)
                    except KeyError:
                        blocks_in_partition[key] = {block_id}

        df = pd.DataFrame(blocks_in_partition.items(), columns=["key", "blocks"])

        size_dict = {
            key: p.initial_size
            for key, p in self.partitions.items()
        }
        with PerformanceLogger("Calc partition size"):
            df["partition_size"] = df["key"].apply(lambda x: size_dict[x])
            size_dict = None

        total_blocks = len(blocks)
        iter_num = 0
        processed_blocks = 0
        processed_blocks_ids = set()
        used_nodes: set[str] = set()
        available_nodes = self.nodes.copy()

        while processed_blocks < total_blocks:
            iter_start_ts = datetime.now(timezone.utc)

            with PerformanceLogger("Apply blocks len"):
                df["block_count"] = df["blocks"].apply(len)

            with PerformanceLogger("Filter df"):
                df = df[df["block_count"] > 0]

            with PerformanceLogger("Extract block ids"):
                df["block_ids"] = df["blocks"].apply(tuple)

            with PerformanceLogger("Aggregate df"):
                agg_df = df[["block_ids", "partition_size", "block_count"]].groupby(
                    by=["block_ids"], sort=False, as_index=False
                ).agg(
                    partition_size=("partition_size", "sum"),
                    block_count=("block_count", "first")
                )

            with PerformanceLogger("Calc weight"):
                agg_df["weight"] = agg_df["partition_size"] / agg_df["block_count"].map(float)

            with PerformanceLogger("Sort by weight"):
                agg_df = agg_df.sort_values(by=["weight", "block_count"], ascending=[False, True], ignore_index=True)# .reset_index(drop=True)

            i = 0
            blocks_ids: list[str] = list()
            score = 0.
            df_len = len(agg_df)
            with PerformanceLogger("Find suitable blocks"):
                blk_id: str
                while i < df_len:
                    blocks_ids = [
                        blk_id for blk_id in agg_df.loc[(i, "block_ids")]
                        if blocks_dict[blk_id].node_id not in used_nodes
                    ]
                    score = agg_df.loc[(i, "weight")]

                    if len(blocks_ids) > 0:
                        break

                    i += 1

            if not blocks_ids:
                raise ValueError("No blocks in set")
            if len(blocks_ids) > 1:
                # To find the most suitable block, we check subset intersections.
                # Repeat until we have exactly one block or until ew have checked the entire subsets.

                with PerformanceLogger("Calc intersection"):
                    i += 1
                    candidates = set(blocks_ids)
                    while len(candidates) > 1 and i < len(agg_df):
                        next_ids = set(agg_df.loc[(i, "block_ids")])

                        if len(candidates.intersection(next_ids)) > 0:
                            candidates.intersection_update(next_ids)

                        if len(candidates) == 1:
                            print(f"Final intersection [i={i}]: {candidates}")
                            break

                        i += 1

                if len(candidates) > 1:
                    # We have checked all the subsets but still have more than one block.
                    # Take the first one.
                    block_id = sorted(list(candidates))[0]
                else:
                    print(f"Most suitable: {candidates}")
                    block_id = list(candidates)[0]

            else:
                print(f"Only one block in set: {blocks_ids}")
                block_id = blocks_ids[0]

            if block_id in processed_blocks_ids:
                raise ValueError(f"Block {block_id} already processed")

            print(f"Iter {iter_num} processed in {datetime.now(timezone.utc) - iter_start_ts}")

            best_block = blocks_dict[block_id]
            yield blocks_dict[block_id], score, iter_num

            with PerformanceLogger("Discard block"):
                df["blocks"].apply(lambda x: x.discard(block_id))

            processed_blocks += 1
            processed_blocks_ids.add(block_id)
            used_nodes.add(best_block.node_id)

            with PerformanceLogger("Update available nodes"):
                if len(used_nodes) == len(available_nodes):
                    used_nodes: set[str] = set()
                    available_nodes = {blk.node_id for blk in blocks if not blk.is_read}
                    iter_num += 1

    def prepare_dumps(self, *, memory_limit: Optional[int] = None, memory_only: bool = False):

        if self.sequence is None:
            raise ValueError("Blocks sequence haven't been determined.")

        blocks: dict[int, list["Block"]] = self.sequence.blocks_sequence
        nodes: set[str] = self.nodes
        partitions = self.partitions

        total_iterations = len(blocks)

        buffers: dict[str, "NodeBuffer"] = {
            node_id: NodeBuffer(
                node_id=node_id,
                total_iterations=total_iterations,
                partitions=partitions,
                memory_limit=memory_limit,
                memory_only=memory_only
            )
            for node_id in nodes
        }

        for iter_num, iter_blocks in sorted(blocks.items()):
            for block in iter_blocks:
                buffers[block.node_id].read_iter_block(iter_num, block, buffers)

            for buffer in buffers.values():
                buffer.apply_iter_data(iter_num)

        self.node_buffers = buffers
