from typing import Hashable, Optional


class Partition:

    __slots__ = (
        "key", "primary_node", "initial_size",
        "current_unread_size", "dumped_size", "buffered_size",
        "complete_iteration", "dump_iters",
    )

    def __init__(self, *,
                 key: Hashable,
                 records_count: int,
                 primary_node: Optional[str] = None):

        self.key: Hashable = key

        self.primary_node: str = primary_node

        self.initial_size: int = records_count

        self.current_unread_size: int = self.initial_size
        self.dumped_size: int = 0
        self.buffered_size: int = 0

        self.complete_iteration: int = -1

        self.dump_iters: dict[int, int] = dict()  # iter_num -> count

    def __len__(self):
        return self.current_unread_size

    def load_to_buffer(self, count: int) -> None:
        self.current_unread_size -= count
        self.buffered_size += count

    @property
    def read_data_size(self):
        return self.initial_size - self.current_unread_size

    def completed(self, num: int):
        self.complete_iteration = num
        self.reset_counters()

    def reset_counters(self):
        self.current_unread_size = self.initial_size
        self.dumped_size = 0
        self.buffered_size = 0

    def dump(self, *,
             iter_num: int,
             count: Optional[int] = None,):

        if count is None:
            count = self.buffered_size

        if count == 0 or count > self.buffered_size:
            raise ValueError(f"Tries to dump {count} records, but only {self.buffered_size} are buffered!")

        current_iter_value = self.dump_iters.get(iter_num, 0)
        self.dump_iters[iter_num] = count + current_iter_value

        self.dumped_size += count
        self.buffered_size -= count
        return count

    def undump(self):
        count = self.dumped_size

        self.buffered_size += count
        self.dumped_size -= count
        return count

    def __repr__(self):
        return (
            f"Partition("
            f"key={self.key}, "
            f"size={self.initial_size}, "
            f"complete_iter={self.complete_iteration}, "
            f"dumped={self.dumped_size} ({self.dump_iters}), "
            f"buffered={self.buffered_size}"
            f")"
        )

    def to_dict(self) -> dict:

        dump_iters_list = [
            {"iter_num": iter_num, "count": count}
            for iter_num, count in self.dump_iters.items()
        ]

        data = {
            "key": self.key,
            "records_count": self.initial_size,
            "primary_node": self.primary_node,
            "complete_iteration": self.complete_iteration,
            "dump_iters_count": dump_iters_list,
        }

        return data

    @staticmethod
    def load_from_args(*,
                       key: Hashable,
                       records_count: int,
                       primary_node: str,
                       complete_iteration: int = -1,
                       dump_iters_count: Optional[list[dict[str, int]]] = None,
                       ) -> "Partition":

        p = Partition(
            key=key,
            records_count=records_count,
            primary_node=primary_node,
        )

        p.complete_iteration = complete_iteration

        if dump_iters_count is not None:
            dump_iters = {
                v["iter_num"]: v["count"]
                for v in dump_iters_count
            }
            p.dump_iters = dump_iters

        return p
