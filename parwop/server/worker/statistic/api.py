from fastapi import APIRouter, Body, HTTPException, Response
from typing import TYPE_CHECKING
import pandas as pd

from parwop.worker.worker import Worker
from parwop.server.common.models import DatasetRequest


if TYPE_CHECKING:
    from parwop.solver.block import Block


router = APIRouter()


@router.post("/")
def calc_statistic(
        partition_by: list[str] = Body(...),
        datasets: list[DatasetRequest] = Body(...),
) -> Response:  # returns Parquet bytes

    if not datasets:
        raise HTTPException(status_code=422, detail="No datasets provided")

    try:
        blocks: list["Block"] = Worker.collect_block_statistics(
            partition_by=partition_by,
            datasets=[ds.to_object() for ds in datasets]
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=422, detail=f"At least one dataset not found!")

    df = pd.DataFrame(
        [b.to_dict(with_statistic=True) for b in blocks]
    )

    return Response(
        content=df.to_parquet(None, index=False),
        media_type="application/octet-stream"
    )
