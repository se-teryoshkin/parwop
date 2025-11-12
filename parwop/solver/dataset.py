import os
from typing import Union, Optional
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, Future
import pandas as pd

from parwop.solver.block import Block
from parwop.settings import MAX_CPU, NODE_ID


class Dataset:

    """
    Base class for a Datasets
    """

    def __init__(self, *,
                 path: str,
                 dataset_id: Optional[str] = None,
                 node_id: Optional[str] = None):

        if dataset_id is None:
            dataset_id = f"{node_id}@{path}"

        self.path: str = path
        self.dataset_id: str = dataset_id
        self.node_id: Optional[str] = node_id

        self.size_in_records: Optional[int] = None
        self.size_in_bytes: Optional[int] = None
        self.blocks: list[Block] = list()
        self.blocks_dict: dict[str, Block] = dict()
        self.is_initialized: bool = False
        self.dataset_type = "Dataset"

    def initialize(self, partition_by: list[str], *args, calculate_statistics: bool = True, **kwargs):
        raise NotImplementedError

    def read_block(self, block: "Block", columns: Optional[list[str]] = None) -> pd.DataFrame:
        raise NotImplementedError

    def set_blocks(self, blocks: list["Block"]) -> None:

        size_in_bytes = 0
        size_in_records = 0
        blocks_dict = dict()

        for b in blocks:
            size_in_bytes += b.size_in_bytes
            size_in_records += b.size_in_records
            blocks_dict[b.block_id] = b

        self.blocks = blocks
        self.size_in_records = size_in_records
        self.size_in_bytes = size_in_bytes
        self.blocks_dict = blocks_dict
        self.is_initialized = True

    def __repr__(self) -> str:
        return (
            f"{self.dataset_type}("
            f"dataset_id={self.dataset_id}, "
            f"size_in_records={self.size_in_records}, "
            f"size_in_bytes={self.size_in_bytes}, "
            f"is_initialized={self.is_initialized})"
        )

    def to_dict(self,
                with_blocks: bool = False,
                with_block_statistics: bool = False,
                debug_info: bool = False) -> dict:

        result: dict[str, Union[float, int, str, list]] = {
            "dataset_id": self.dataset_id,
            "path": self.path,
            "node_id": self.node_id,
            "dataset_type": self.dataset_type,
        }

        if with_blocks:
            result["blocks"] = [
                b.to_dict(with_statistic=with_block_statistics, debug_info=debug_info)
                for b in self.blocks
            ]

        if debug_info:
            result.update({
                "size_in_records": self.size_in_records,
                "size_in_bytes": self.size_in_bytes,
            })

        return result

class ParquetDataset(Dataset):

    def __init__(self, *,
                 path: str,
                 dataset_id: Optional[str] = None,
                 node_id: Optional[str] = None):

        super().__init__(path=path, dataset_id=dataset_id, node_id=node_id)
        self.dataset_type = "ParquetDataset"

    def initialize(self,
                   partition_by: list[str],
                   calculate_statistics: bool = True,
                   shard_num: Optional[int] = None,
                   total_shards: Optional[int] = None,
                   ) -> list[Block]:

        path = self.path

        if not os.path.exists(path):
            raise FileNotFoundError(path)

        if os.path.isdir(path):
            files_list = [os.path.join(path, file) for file in os.listdir(path) if file.endswith(".parquet")]

            if shard_num is not None:
                if total_shards is None:
                    raise ValueError(f"Sharding is enabled, but total shards number is not passed!")

                files_to_process = files_list[shard_num::total_shards]
            else:
                files_to_process = files_list
        else:
            if not path.endswith(".parquet"):
                raise ValueError(f"File path {path} should ends with `.parquet` extension")
            files_to_process = [path]

        size_in_bytes_files = {
            file: os.path.getsize(file)
            for file in files_to_process
        }

        blocks: list["Block"] = list()
        futures: dict[str, Future["pd.Series[int]"]] = dict()
        dataset_id = self.dataset_id
        node_id = self.node_id

        if node_id is None:
            node_id = NODE_ID

        if calculate_statistics:
            with ProcessPoolExecutor(max_workers=min(MAX_CPU, len(files_to_process))) as concurrent_pool:
                for file in files_to_process:
                    futures[file] = concurrent_pool.submit(
                        self.calculate_statistics,
                        file_path=file,
                        columns=partition_by
                    )

        for file in files_to_process:
            if calculate_statistics:
                stats: "pd.Series[int]" = futures[file].result()
            else:
                stats = pd.Series()

            records_count = stats.sum()

            blocks.append(
                Block(
                    dataset_id=dataset_id,
                    node_id=node_id,
                    block_id=f"{node_id}:{dataset_id}:{file}",
                    size_in_records=records_count,
                    size_in_bytes=size_in_bytes_files[file],
                    statistics=stats
                )
            )

        self.set_blocks(blocks)

        return blocks

    @staticmethod
    def calculate_statistics(file_path: str, columns: list[str]) -> "pd.Series[int]":
        df = pd.read_parquet(file_path, columns=columns)
        return df.groupby(by=columns, sort=False).size()

    def read_block(self, block: "Block", columns: Optional[list[str]] = None) -> pd.DataFrame:
        if block.block_id not in self.blocks_dict:
            raise ValueError(f"Block {block.block_id} is not found in dataset {self.dataset_id}!")
        
        file_path = block.block_id.split(":")[-1]
        return pd.read_parquet(file_path, columns=columns)


DatasetType = Union[Dataset, ParquetDataset]
DATASET_TYPES: dict[str, type[DatasetType]] = {
    "Dataset": Dataset,
    "ParquetDataset": ParquetDataset,
}
