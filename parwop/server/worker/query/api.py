import json
from io import BytesIO
from streaming_form_data.targets import BaseTarget
from streaming_form_data import StreamingFormDataParser
from fastapi import APIRouter, BackgroundTasks, HTTPException, Response, Form, UploadFile, File, Request
from fastapi_utils.tasks import repeat_every
import pandas as pd
from typing import Optional

from parwop.server.common.models import QueryState, QueryParams
from parwop.server.common.exceptions import QueryAlreadyExists
from parwop.server.worker.query.service import worker_query_service, WorkerQuery
from parwop.server.worker.query.service import report_status_to_coordinator

router = APIRouter()


@repeat_every(seconds=60)
def check_if_queries_timed_out() -> None:
    worker_query_service.remove_timed_out_queries()


@router.post("/{query_id}/")
def prepare_query(
        background: BackgroundTasks,
        query_id: str,
        partition_by: bytes = Form(...),
        order_by: bytes = Form(...),
        datasets_metadata: UploadFile = File(...),
        blocks_metadata: UploadFile = File(...),
        partitions_metadata: UploadFile = File(...),
        node_iterations_metadata: UploadFile = File(...),
        worker_kwargs: Optional[bytes] = Form(None),
):

    if worker_query_service.get_query(query_id) is not None:
        raise HTTPException(status_code=422, detail=f"Query {query_id} already exists.")

    query = WorkerQuery(
        query_id=query_id,
        state=QueryState.PREPARING_WORKERS,
        params=QueryParams(
            partition_by=json.loads(partition_by.decode("utf-8")),
            order_by=json.loads(order_by.decode("utf-8")),
            datasets=list(),
            worker_kwargs=json.loads(worker_kwargs.decode("utf-8")) if worker_kwargs else None,
        )
    )

    try:
        worker_query_service.put_query(query)
    except QueryAlreadyExists:
        raise HTTPException(status_code=422, detail=f"Query {query_id} already exists.")

    background.add_task(
        worker_query_service.prepare_query,
        query=query,
        datasets_metadata=pd.read_parquet(datasets_metadata.file),
        blocks_metadata=pd.read_parquet(blocks_metadata.file),
        partitions_metadata=pd.read_parquet(partitions_metadata.file),
        node_iterations_metadata=pd.read_parquet(node_iterations_metadata.file),
    )


@router.get("/")
def list_queries() -> dict[str, WorkerQuery]:
    return worker_query_service.get_queries()


@router.get("/{query_id}/")
def get_query(query_id: str) -> WorkerQuery:
    query = worker_query_service.get_query(query_id)
    if query is None:
        raise HTTPException(status_code=404, detail=f"Query {query_id} not found")

    return query


@router.post("/{query_id}/start/")
def start_prepared_query(query_id: str, background: BackgroundTasks) -> None:

    query = worker_query_service.get_query(query_id)
    if query is None:
        raise HTTPException(status_code=404, detail=f"Query {query_id} not found")

    if query.state != QueryState.READY:
        raise HTTPException(
            status_code=422,
            detail=f"Query {query_id} state should be {QueryState.READY}, but current state is {query.state}"
        )

    background.add_task(worker_query_service.start_query, query)


@router.get("/{query_id}/worker_stats/")
def get_worker_stats(query_id: str) -> dict:

    query = worker_query_service.get_query(query_id)
    if query is None:
        raise HTTPException(status_code=404, detail=f"Query {query_id} not found")

    if query.worker is None:
        raise HTTPException(status_code=404, detail=f"Worker for query {query_id} not found")

    # TODO: Implement query stats fetch
    return {}


@router.delete("/{query_id}/")
def stop_query(query_id: str, background: BackgroundTasks) -> None:

    query = worker_query_service.get_query(query_id)
    if query is None:
        raise HTTPException(status_code=404, detail=f"Query {query_id} not found")

    background.add_task(worker_query_service.stop_query, query)


@router.get("/{query_id}/batch/{batch_num}/")
def get_data_batch(query_id: str, batch_num: int, background: BackgroundTasks):

    query = worker_query_service.get_query(query_id)
    if query is None:
        raise HTTPException(status_code=404, detail=f"Query {query_id} not found")

    if query.state in QueryState.TERMINAL_STATES:
        raise HTTPException(status_code=422, detail=f"Query is in terminal state {query.state}")

    if (worker := query.worker) is None:
        raise HTTPException(status_code=404, detail=f"Worker for query {query_id} not found")

    batch: Optional[bytes] = worker.get_data_batch(batch_num=batch_num)

    if worker.is_query_completed and len(worker.output_batches) == 0:
        query.state = QueryState.FINISHED
        background.add_task(report_status_to_coordinator, query)
        worker_query_service.remove_worker(query)

    status_code = 200 if batch else 404

    return Response(
        content=batch,
        media_type="application/octet-stream",
        status_code=status_code
    )


# Class for Shuffle endpoint stream
class FileTargetInMemory(BaseTarget):
    """FileTarget writes (streams) the input to in-memory BytesIO."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file = None

    def on_start(self):
        self.file: BytesIO = BytesIO()

    def on_data_received(self, chunk: bytes):
        if self.file:
            self.file.write(chunk)

    def on_finish(self):
        # Warning: User should manually close the file!
        pass


@router.post("/{query_id}/shuffle/{iter_num}/")
async def upload_in_memory(
        query_id: str,
        iter_num: int,
        request: Request,
        background: BackgroundTasks
):

    query = worker_query_service.get_query(query_id)
    if query is None:
        raise HTTPException(status_code=404, detail=f"Query {query_id} not found")

    if query.worker is None:
        raise HTTPException(status_code=404, detail=f"Worker for query {query_id} not found")

    # Raw request stream is used. Reason:
    # https://stackoverflow.com/questions/65342833/fastapi-uploadfile-is-slow-compared-to-flask/70667530#70667530
    # https://stackoverflow.com/questions/73442335/how-to-upload-a-large-file-%e2%89%a53gb-to-fastapi-backend/73443824#73443824
    shuffle_data_ft = FileTargetInMemory()

    parser = StreamingFormDataParser(headers=request.headers)
    parser.register("data", shuffle_data_ft)

    async for chunk in request.stream():
        parser.data_received(chunk)

    background.add_task(
        worker_query_service.input_shuffle,
        query=query,
        iter_num=iter_num,
        data=shuffle_data_ft.file,
    )
