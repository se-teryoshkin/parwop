from typing import Hashable, TYPE_CHECKING, Optional
import pandas as pd

from parwop.utils import PerformanceLoggerDecorator
from parwop.settings import DEFAULT_DUMP_BATCH_SIZE

if TYPE_CHECKING:
    from parwop.solver.block import Block  # noqa
    from parwop.solver.partition import Partition  # noqa


class GlobalBuffer:

    def __init__(self, *,
                 partitions: dict[Hashable, "Partition"]):


        self.partitions: dict[Hashable, "Partition"] = partitions
        self.buffered_size = 0
        self.max_size = 0

    def load_to_buffer(self, *, block: "Block") -> tuple[list["Partition"], "pd.Series[int]"]:

        statistic: "pd.Series[int]" = block.statistic
        partitions = self.partitions
        completed_partitions: list["Partition"] = list()

        for key, count in statistic.items():
            partition = partitions[key]

            partition.load_to_buffer(count)

            self.increment(count)
            if partition.current_unread_size == 0:
                completed_partitions.append(partition)

        for partition in completed_partitions:
            self.decrement(partition.initial_size)

        return completed_partitions, statistic

    def increment(self, count):
        buffered_size = self.buffered_size
        max_size = self.max_size

        buffered_size += count
        if buffered_size > max_size:
            max_size = buffered_size

        self.buffered_size = buffered_size
        self.max_size = max_size

    def decrement(self, count):
        self.buffered_size -= count

    def __repr__(self):
        return (
            f"GlobalBuffer("
            f"current_size={self.buffered_size}, "
            f"partitions={len(self.partitions.keys())}, "
            f"max_size={self.max_size})"
        )

    def remove_partitions_from_buffer(self, *, partitions: list["Partition"]):
        for p in partitions:
            self.buffered_size -= p.initial_size

    def __len__(self):
        return self.buffered_size


class IterationInfo:

    __slots__ = ("iter_num", "node_id", "block_id", "block_size_in_records", "input_shuffle_count",
                 "output_shuffle_count", "use_direct_buffer_return", "sortmerge_partitions_read_to_return_data",
                 "read_dump", "write_dump", "returned_count", "partitions", "peak_buffer_size")

    def __init__(self, iter_num: int, node_id: str):
        self.iter_num = iter_num
        self.node_id = node_id
        self.block_id: Optional[str] = None
        self.block_size_in_records = 0
        self.input_shuffle_count = 0
        self.output_shuffle_count = 0
        self.partitions: dict[Hashable, int] = dict()

        # TODO: Do we need it here?
        self.use_direct_buffer_return = False
        self.sortmerge_partitions_read_to_return_data: Optional[list[int]] = None

        self.read_dump = 0
        self.write_dump = 0
        self.peak_buffer_size = 0
        self.returned_count = 0

    def _put(self, partition: "Partition", count: int):
        partitions = self.partitions
        key = partition.key
        try:
            partitions[key] += count
        except KeyError:
            partitions[key] = count

    def read_from_block(self, partition: "Partition", count: int):
        self._put(partition, count)

    def input_shuffle(self, partition: "Partition", count: int):
        self.input_shuffle_count += count
        self._put(partition, count)

    def to_dict(self, debug_info: bool = False) -> dict:
        result = {
            "iter_num": self.iter_num,
            "block_id": self.block_id,
            "input_shuffle_count": self.input_shuffle_count,
            "output_shuffle_count": self.output_shuffle_count,
            "use_direct_buffer_return": self.use_direct_buffer_return,
            "sortmerge_partitions_read_to_return_data": self.sortmerge_partitions_read_to_return_data,
            "read_dump": self.read_dump,
            "write_dump": self.write_dump,
            "returned_count": self.returned_count,
        }

        if debug_info:
            result.update({
                "node_id": self.node_id,
                "block_size_in_records": self.block_size_in_records,
                "peak_buffer_size": self.peak_buffer_size,
            })

        return result

    @staticmethod
    @PerformanceLoggerDecorator("Load Iteration Info Metadata")
    def load_metadata(df: "pd.DataFrame", node_id: str) -> dict[int, "IterationInfo"]:

        result: dict[int, "IterationInfo"] = dict()

        for (
                iter_num, block_id, input_shuffle_count, output_shuffle_count,
                use_direct_buffer_return, sortmerge_partitions_read_to_return_data, read_dump,
                write_dump, returned_count
        ) in zip(
            df["iter_num"].values,
            df["block_id"].values,
            df["input_shuffle_count"].values,
            df["output_shuffle_count"].values,
            df["use_direct_buffer_return"].values,
            df["sortmerge_partitions_read_to_return_data"].values,
            df["read_dump"].values,
            df["write_dump"].values,
            df["returned_count"].values,
        ):

            iter_info = IterationInfo(iter_num, node_id)

            iter_info.block_id = block_id
            iter_info.input_shuffle_count = input_shuffle_count
            iter_info.output_shuffle_count = output_shuffle_count
            iter_info.use_direct_buffer_return = use_direct_buffer_return

            iter_info.sortmerge_partitions_read_to_return_data = sortmerge_partitions_read_to_return_data

            # TODO: Add block_size_in_records

            iter_info.read_dump = read_dump
            iter_info.write_dump = write_dump
            iter_info.returned_count = returned_count

            result[iter_num] = iter_info

        return result


class NodeBuffer:

    def __init__(self, *,
                 node_id: str,
                 total_iterations: int,
                 partitions: dict[Hashable, "Partition"],
                 memory_limit: int,
                 memory_only: bool,
                 pessimistic_shuffle_strategy: bool = False,
                 sortmerge_batch_size: int = DEFAULT_DUMP_BATCH_SIZE,
                 ):

        self.node_id = node_id
        self.partitions = partitions
        self.sortmerge_batch_size = sortmerge_batch_size

        self.node_partitions: dict[Hashable, "Partition"] = {
            key: p
            for key, p in partitions.items()
            if p.primary_node == node_id
        }

        partition_complete_iterations: dict[int, list["Partition"]] = dict()
        for key, partition in self.node_partitions.items():
            complete_iter = partition.complete_iteration
            try:
                partition_complete_iterations[complete_iter].append(partition)
            except KeyError:
                partition_complete_iterations[complete_iter] = [partition]

        for iter_num in range(total_iterations):
            if iter_num not in partition_complete_iterations:
                partition_complete_iterations[iter_num] = list()

        self.partition_complete_iterations = partition_complete_iterations

        self.memory_limit = memory_limit
        self.memory_only = memory_only
        self.pessimistic_shuffle_strategy = pessimistic_shuffle_strategy
        self.max_buffer_size = 0

        self.iterations_data: dict[int, "IterationInfo"] = dict()

        self.buffered_partitions: dict[Hashable, "Partition"] = dict()
        self.dumped_partitions: dict[Hashable, "Partition"] = dict()

    @property
    def current_buffer_size(self):
        return sum([p.buffered_size for p in self.node_partitions.values()])

    def get_iteration_data(self, iter_num: int) -> "IterationInfo":

        if iter_num not in self.iterations_data:
            data = IterationInfo(iter_num, self.node_id)
            self.iterations_data[iter_num] = data
        else:
            data = self.iterations_data[iter_num]

        return data

    def read_iter_block(self, iter_num: int, block: "Block", buffers: dict[str, "NodeBuffer"]):
        block_statistics = block.statistic
        all_partitions = self.partitions
        node_partitions = self.node_partitions
        iter_data = self.get_iteration_data(iter_num)

        iter_data.block_id = block.block_id
        iter_data.block_size_in_records = block.size_in_records

        for key, count in block_statistics.items():
            partition = all_partitions[key]
            if key in node_partitions:
                iter_data.read_from_block(partition=partition, count=count)
            else:
                iter_data.output_shuffle_count += count
                buffers[partition.primary_node].get_iteration_data(iter_num).input_shuffle(
                    partition=partition, count=count
                )

    def apply_iter_data(self, iter_num: int):

        iter_data = self.get_iteration_data(iter_num)
        memory_limit = self.memory_limit
        memory_only = self.memory_only
        pessimistic_shuffle_strategy = self.pessimistic_shuffle_strategy
        peak_buffer_size = self.current_buffer_size + iter_data.block_size_in_records + iter_data.input_shuffle_count

        total_dumped_count = 0

        if not pessimistic_shuffle_strategy:
            peak_buffer_size -= iter_data.output_shuffle_count

        if memory_limit > 0:
            # Memory limit is set

            if peak_buffer_size > memory_limit:
                print(f"Peak memory usage > memory limit: {peak_buffer_size} > {memory_limit}")
                if memory_only:
                    raise RuntimeError(
                        f"Unable to perform query using only memory! "
                        f"Peak memory usage {peak_buffer_size} > {memory_limit} memory limit"
                    )

                dumped_count = self.preemptive_dump(iter_data, peak_buffer_size - memory_limit)
                peak_buffer_size -= dumped_count
                total_dumped_count += dumped_count
                print(f"Dumped {dumped_count} records ({peak_buffer_size}/{memory_limit})")

            should_be_read_from_dump = sum(
                [p.dumped_size for p in self.partition_complete_iterations[iter_num]]
            )

            if should_be_read_from_dump > 0:

                if peak_buffer_size + should_be_read_from_dump > memory_limit:

                    returned_from_buffer_count = self.remove_completed_partitions(iter_data, intermediate=True)

                    if returned_from_buffer_count > 0:
                        peak_buffer_size -= returned_from_buffer_count
                        iter_data.use_direct_buffer_return = True
                        print(
                            f"Directly returned from buffer: {returned_from_buffer_count} "
                            f"({peak_buffer_size}/{memory_limit})"
                        )

                if peak_buffer_size + should_be_read_from_dump > memory_limit:
                    # The data from the dump cannot fit in memory.
                    # That's why we will use sortmerge reading.
                    print(f"Using sortmerge algorithm for reading dumps: {peak_buffer_size=}, {should_be_read_from_dump=}")
                    partitions_count, preemptive_dumped_count, required_buffer_size = (
                        self.read_dump_sortmerge(iter_data, peak_buffer_size)
                    )

                    # TODO: Correct calculate `should_be_read_from_dump`
                    peak_buffer_size -= preemptive_dumped_count
                    peak_buffer_size += required_buffer_size
                    total_dumped_count += preemptive_dumped_count
                    iter_data.sortmerge_partitions_read_to_return_data = partitions_count
                else:
                    # We can read the entire dump to the buffer.
                    _ = self.read_dump_naive(iter_data)
                    peak_buffer_size += should_be_read_from_dump

                iter_data.read_dump = should_be_read_from_dump

        iter_data.peak_buffer_size = peak_buffer_size
        iter_data.write_dump = total_dumped_count
        iter_data.returned_count = sum(
            [p.initial_size for p in self.partition_complete_iterations[iter_num]]
        )

        if self.max_buffer_size < peak_buffer_size:
            self.max_buffer_size = peak_buffer_size

        self.merge_buffers(iter_data)
        self.remove_completed_partitions(iter_data, intermediate=False)

        self.print_info(iter_data)

    def preemptive_dump(self, iter_data: "IterationInfo", count: int) -> int:

        iter_num = iter_data.iter_num
        buffered_partitions = [p for p in self.node_partitions.values() if p.buffered_size > 0]

        write_dump_counter = 0
        for p in sorted(
                [p for p in buffered_partitions if p.complete_iteration > iter_num],
                key=lambda p: (p.complete_iteration, p.buffered_size),
                reverse=True
        ):
            write_dump_counter += p.dump(iter_num=iter_num)
            if write_dump_counter >= count:
                break

        enable_sortmerge = False
        if write_dump_counter < count:
            print("WARNING! Dump partitions that will be returned on this iteration. Enable sortmerge algorithm.")
            enable_sortmerge = True
            for p in sorted(
                    [p for p in buffered_partitions if p.complete_iteration == iter_num],
                    key=lambda p: p.key,
                    reverse=True
            ):
                write_dump_counter += p.dump(iter_num=iter_num)
                if write_dump_counter >= count:
                    break

        if write_dump_counter < count and not enable_sortmerge:
            raise ValueError(
                f"Unable to prepare dump! Should be dumped {count}, but all the buffered data is {write_dump_counter}"
            )

        return write_dump_counter

    def merge_buffers(self, iter_data: "IterationInfo") -> None:
        partitions = self.node_partitions
        all_partitions = self.partitions

        for key, count in iter_data.partitions.items():
            try:
                partitions[key].load_to_buffer(count)
            except KeyError as e:
                p = all_partitions[key]

                # We already removed this partition in `remove_completed_partitions` during intermediate return
                if p.buffered_size + count == p.initial_size:
                    p.load_to_buffer(count)
                else:
                    # Otherwise something goes wrong.
                    raise e

        iter_data.partitions = dict()  # Just to reduce memory consumptions

    def remove_completed_partitions(self, iter_data: "IterationInfo", intermediate: bool = True) -> int:

        completed_partitions = self.partition_complete_iterations[iter_data.iter_num]
        node_partitions = self.node_partitions
        iter_data_partitions = iter_data.partitions
        removed_counter = 0

        for partition in completed_partitions:
            key = partition.key
            if key not in node_partitions:
                # Already removed earlier
                continue

            partition_size = partition.initial_size

            if partition.buffered_size + iter_data_partitions.get(key, 0) != partition_size:
                if intermediate:
                    continue
                raise ValueError(f"Partition {key} is not fully buffered: {partition}.")

            node_partitions.pop(key)

            removed_counter += partition_size

        return removed_counter

    def read_dump_naive(self, iter_data: "IterationInfo") -> int:

        completed_partitions = self.partition_complete_iterations[iter_data.iter_num]
        read_counter = 0

        for partition in completed_partitions:
            if partition.dumped_size > 0:
                read_counter += partition.undump()

        return read_counter

    def read_dump_sortmerge(self, iter_data: "IterationInfo", peak_buffer_size: int) -> tuple[list[int], int, int]:

        dumped_completed_partitions = [
            p
            for p in self.partition_complete_iterations[iter_data.iter_num]
            if p.dumped_size > 0
        ]
        batch_size = self.sortmerge_batch_size
        memory_limit = self.memory_limit

        files_length: dict[int, int] = dict()
        for partition in dumped_completed_partitions:
            for file, count in partition.dump_iters.items():
                try:
                    files_length[file] += count
                except KeyError:
                    files_length[file] = count

        sorted_completed_partitions = sorted(dumped_completed_partitions, key=lambda p: p.key)

        size = sum([min(files_length[file], batch_size) for file in sorted_completed_partitions[0].dump_iters.keys()])

        desired_dump_count = 0
        dumped_count = 0
        if peak_buffer_size + size > memory_limit:
            desired_dump_count = peak_buffer_size + size - memory_limit
            dumped_count = self.preemptive_dump(iter_data, desired_dump_count)
            peak_buffer_size -= dumped_count
            print(f"Dumped (for sort-merge desired - {desired_dump_count}) {dumped_count} records "
                  f"({peak_buffer_size}/{memory_limit})")

            # Update files info because after preemptive dumps new data might appear.
            dumped_completed_partitions = [
                p
                for p in self.partition_complete_iterations[iter_data.iter_num]
                if p.dumped_size > 0
            ]
            files_length: dict[int, int] = dict()
            for partition in dumped_completed_partitions:
                for file, count in partition.dump_iters.items():
                    try:
                        files_length[file] += count
                    except KeyError:
                        files_length[file] = count

        data_offsets: dict[int, int] = dict()
        read_records: dict[int, int] = dict()

        available_memory_delta = memory_limit - peak_buffer_size

        if available_memory_delta <= 0:
            raise ValueError(f"No available memory for sort-merge dump reading: {peak_buffer_size}/{memory_limit}")

        partitions_count_list = list()
        partitions_counter = 0

        for partition in sorted_completed_partitions:

            for file, count in partition.dump_iters.items():

                read_from_file = read_records.get(file, 0)
                data_offset = data_offsets.get(file, 0)

                if read_from_file < data_offset + count:

                    # Read the new batch
                    read_size = min(batch_size, files_length[file] - read_from_file)
                    read_from_file += read_size
                    available_memory_delta -= read_size

                read_records[file] = read_from_file
                data_offsets[file] = data_offset + count

            if available_memory_delta < 0:
                # We will not be able to process this partition, that's why we have to dump previous read
                removed_count = self.remove_completed_partitions(iter_data, intermediate=True)
                partitions_count_list.append(partitions_counter)
                available_memory_delta += removed_count

                if available_memory_delta < 0:
                    # Something goes wrong. Out memory buffer still overflow even after partitions removal.
                    # Probably the next partition requires other dump files.
                    # TODO: Add preemptive dumps?
                    print(f"WARNING! Buffer overflows for {-1 * available_memory_delta} records!")

            partitions_counter += 1
            partition.undump()

        return partitions_count_list, dumped_count, desired_dump_count

    def print_info(self, iter_data: "IterationInfo") -> None:

        str_res = (
            f"NodeBuffer("
            f"iter_num={iter_data.iter_num}, "
            f"node_id={self.node_id}, "
            f"buffer_size={self.current_buffer_size}, "  # After iteration
            f"buffer_peak_size={iter_data.peak_buffer_size}, "  # During iteration
            f"max_buffer_size={self.max_buffer_size}, "
            f"iter_dumped={iter_data.write_dump}, "
            f"iter_undumped={iter_data.read_dump}, "
            f"returned_records={iter_data.returned_count}"
            f")"
        )
        print(str_res)

    def get_metadata(self) -> "pd.DataFrame":
        return pd.DataFrame(
            [iter_data.to_dict() for iter_data in self.iterations_data.values()]
        ).sort_values(
            by=["iter_num"], ascending=True, ignore_index=True
        ).reset_index(drop=True)
