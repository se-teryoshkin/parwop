from typing import Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:
    import pandas as pd  # noqa


class Block:

    __slots__ = (
        "node_id", "dataset_id", "block_id", "size_in_records", "size_in_bytes",
        "statistic", "is_read", "read_on_iteration", "score"
    )

    def __init__(self, *,
                 dataset_id: str,
                 block_id: str,
                 node_id: str,
                 size_in_records: int = 0,
                 size_in_bytes: int = 0,
                 statistics: Optional["pd.Series[int]"] = None):

        self.node_id = node_id
        self.dataset_id = dataset_id

        self.block_id = block_id

        self.size_in_records = size_in_records
        self.size_in_bytes = size_in_bytes

        if statistics is not None:
            if statistics.index.names is not None:
                statistics.index = statistics.index.to_flat_index()

            statistics.rename_axis("key", inplace=True)
            statistics.name = "count"

        self.statistic = statistics

        self.is_read: bool = False
        self.read_on_iteration: int = -1
        self.score: float = -1.

    def mark_as_read(self, iter_num: int, score: float):
        self.is_read = True
        self.read_on_iteration = iter_num
        self.score = score

    def to_dict(self,
                with_statistic: bool = False,
                debug_info: bool = False) -> dict:

        result: dict[str, Union[float, int, str, list]] = {
            "block_id": self.block_id,
            "dataset_id": self.dataset_id,
            "node_id": self.node_id,
            "size_in_records": self.size_in_records,
        }

        if with_statistic:
            result["statistic"] = [
                {
                    "key": key,
                    "count": count
                }
                for key, count in self.statistic.items()
            ]

        if debug_info:
            result.update({  # noqa
                "size_in_bytes": self.size_in_bytes,
                "score": self.score,
                "read_on_iteration": self.read_on_iteration,
            })

        return result

    def __len__(self):
        return self.size_in_records

    def __repr__(self):
        return (
            f"Block("
            f"node_id={self.node_id}, "
            f"dataset_id={self.dataset_id}, "
            f"block_id={self.block_id}, "
            f"size_in_records={self.size_in_records}, "
            f"size_in_byts={self.size_in_bytes}, "
            f"score={self.score}"
            f")"
        )
