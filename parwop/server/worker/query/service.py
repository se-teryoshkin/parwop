from datetime import datetime, timezone
from functools import wraps
from io import BytesIO
from typing import Optional, TYPE_CHECKING, Callable
import traceback
import requests
from pydantic import BaseModel, Field, ConfigDict

from parwop.utils import PerformanceLogger, get_rss, trim_memory
from parwop.server.common.models import QueryState, QueryParams
from parwop.server.common.exceptions import QueryAlreadyExists
from parwop.settings import NODE_ID
from parwop.worker.worker import Worker
from parwop.server.discovery import discovery_service

if TYPE_CHECKING:
    import pandas as pd


class WorkerQuery(BaseModel):
    query_id: str
    state: QueryState
    params: QueryParams
    detail: Optional[str] = None

    worker: Optional[Worker] = Field(None, exclude=True)

    model_config = ConfigDict(arbitrary_types_allowed=True)


def notify_coordinator_on_exception(func: Callable):

    @wraps(func)
    def wrapper(*args, **kwargs):

        if (query := kwargs.get("query", None)) is None:
            for arg in args:
                if isinstance(arg, WorkerQuery):
                    query = arg
                    break

        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_text = traceback.format_exc()
            print(f"Exception on execution {func.__name__}:\n {error_text}")

            query.state = QueryState.ERROR
            query.detail = error_text

            worker_query_service.remove_worker(query)

            report_status_to_coordinator(query)

    return wrapper


def report_status_to_coordinator(query: WorkerQuery):
    if (coordinator_address := discovery_service.coordinator_address) is not None:
        response = requests.put(
            f"{coordinator_address}/coordinator/query/{query.query_id}/worker_status",
            json={
                "node_id": discovery_service.this_node_status.node_id,
                "state": query.state,
                "detail": query.detail,
            }
        )

        if response.status_code != 200:
            print(f"WARNING! Response status code ({response.status_code}): {response.content}")


class WorkerQueryService:

    def __init__(self):
        self.queries: dict[str, WorkerQuery] = dict()

    def get_queries(self) -> dict[str, WorkerQuery]:
        return self.queries

    def get_query(self, query_id: str) -> Optional[WorkerQuery]:
        return self.queries.get(query_id, None)

    def put_query(self, query: WorkerQuery):
        if query.query_id in self.queries:
            raise QueryAlreadyExists(f"Query {query.query_id} already exists")

        self.queries[query.query_id] = query

    @notify_coordinator_on_exception
    def prepare_query(
            self,
            query: WorkerQuery,
            datasets_metadata: "pd.DataFrame",
            blocks_metadata: "pd.DataFrame",
            partitions_metadata: "pd.DataFrame",
            node_iterations_metadata: "pd.DataFrame",
    ) -> None:

        print(f"Before Worker creation rss: {get_rss(True)}")

        query.state = QueryState.LOADING
        params = query.params

        with PerformanceLogger("Create Worker"):
            worker = Worker(
                node_id=NODE_ID,
                partition_by=params.partition_by,
                order_by=params.order_by,
                query_id=query.query_id,
                datasets_metadata=datasets_metadata,
                blocks_metadata=blocks_metadata,
                partitions_metadata=partitions_metadata,
                node_iterations_metadata=node_iterations_metadata,
                # TODO: Make model for worker_kwargs
                **params.worker_kwargs
            )

        print(f"After Worker creation rss: {get_rss(True)}")

        query.worker = worker
        query.state = QueryState.READY

        report_status_to_coordinator(query)

    @notify_coordinator_on_exception
    def start_query(self, query: WorkerQuery) -> None:

        start_time = datetime.now(timezone.utc)

        worker = query.worker
        query.state = QueryState.RUNNING

        report_status_to_coordinator(query)

        print(f"Before run rss: {get_rss(True)}")

        try:
            worker.run()
        except Exception as e:
            if worker is not None:
                worker.is_cancelled = True
            raise e
        else:
            end_time = datetime.now(timezone.utc)
            print(f"Request is finished in {end_time - start_time}")

            if worker.is_cancelled:
                query.state = QueryState.TIMEOUT if worker.is_query_timed_out else QueryState.CANCELLED
                self.remove_worker(query)

            else:
                if worker.read_only or (worker.is_query_completed and len(worker.output_batches) == 0):
                    query.state = QueryState.FINISHED
                else:
                    query.state = QueryState.COMPLETED

            report_status_to_coordinator(query)

            if query.state == QueryState.FINISHED:
                self.remove_worker(query)

            print(f"After finish rss: {get_rss(True)}")

    def stop_query(self, query: WorkerQuery) -> None:

        if (worker := query.worker) is None or query.state in QueryState.TERMINAL_STATES:
            return

        worker: Worker
        query.state = QueryState.TIMEOUT if worker.is_query_timed_out else QueryState.CANCELLED

        self.remove_worker(query)

    @staticmethod
    def remove_worker(query: WorkerQuery) -> None:
        query.worker = None
        trim_memory()

    @notify_coordinator_on_exception
    def input_shuffle(self, query: WorkerQuery, iter_num: int, data: BytesIO) -> None:
        worker = query.worker
        try:
            worker.input_shuffle(iter_num=iter_num, data=data)
        finally:
            data.close()

    def remove_timed_out_queries(self):
        for query_id, query in self.queries.items():
            if (worker := query.worker) is None:
                continue

            worker: Worker
            if (
                query.state in QueryState.RUNNING_STATES
                and worker.query_timeout > 0
                and (worker.start_time.timestamp() + worker.query_timeout) <= (now := datetime.now(timezone.utc)).timestamp()
            ):
                print(
                    f"WARNING! Query {query_id} is timed out. "
                    f"Start time = {worker.start_time}, "
                    f"timeout = {worker.query_timeout}, Now = {now}"
                )

                worker.is_cancelled = True
                worker.is_query_timed_out = True

                query.state = QueryState.TIMEOUT

                report_status_to_coordinator(query)

                worker: None = None
                self.remove_worker(query)


worker_query_service = WorkerQueryService()
