import json
import traceback
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Callable, TYPE_CHECKING
from datetime import datetime, timezone
import requests

from parwop.server.common.models import QueryState, QueryParams
from parwop.server.common.exceptions import QueryAlreadyExists
from parwop.solver.query import QueryDriver
from parwop.server.discovery import discovery_service
from parwop.server.mq import mq_service
from parwop.utils import trim_memory


if TYPE_CHECKING:
    import pandas as pd


class WorkerStatus(BaseModel):
    node_id: str
    state: QueryState
    detail: Optional[str] = None


class Query(BaseModel):
    query_id: str
    state: QueryState
    workers: dict[str, WorkerStatus]
    params: QueryParams
    detail: Optional[str] = None
    start_ts: Optional[datetime] = None
    finish_ts: Optional[datetime] = None
    duration: Optional[float] = None

    driver: Optional[QueryDriver] = Field(None, exclude=True)

    model_config = ConfigDict(arbitrary_types_allowed=True)


def notify_clients_on_exception(func: Callable):

    @wraps(func)
    def wrapper(*args, **kwargs):

        if (query := kwargs.get("query", None)) is None:
            for arg in args:
                if isinstance(arg, Query):
                    query = arg
                    break

        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_text = traceback.format_exc()
            print(f"Exception on execution {func.__name__}:\n {error_text}")

            query.state = QueryState.ERROR
            query.detail = error_text
            query.driver = None
            trim_memory()

            mq_service.send_msg_to_consumers(topic=query.query_id, msg={"error": error_text})

    return wrapper


class CoordinatorQueryService:

    def __init__(self):
        self.queries: dict[str, Query] = dict()

    def get_queries(self) -> dict[str, Query]:
        return self.queries

    def get_query(self, query_id: str) -> Optional[Query]:
        return self.queries.get(query_id, None)

    def put_query(self, query: Query):
        if query.query_id in self.queries:
            raise QueryAlreadyExists(f"Query {query.query_id} already exists")

        self.queries[query.query_id] = query

    @notify_clients_on_exception
    def run_query(self, query: Query) -> None:

        query_params = query.params

        driver = QueryDriver(
            datasets=[ds.to_object() for ds in query_params.datasets],
            partition_by=query_params.partition_by,
            order_by=query_params.order_by,
            memory_limit=query_params.memory_limit_records,
            is_distributed=True,
            query_id=query.query_id,
            worker_kwargs=query_params.worker_kwargs,
            solver_kwargs=query_params.solver_kwargs,
        )

        query.driver = driver
        query.state = QueryState.PLAN_BUILDING

        driver.build_plans()

        self.prepare_query_on_workers(query)

        driver.clear_driver_metadata()

    def prepare_query_on_workers(self, query: Query) -> None:

        driver = query.driver

        datasets_metadata, blocks_metadata, partitions_metadata, node_iterations_metadata = driver.get_metadata()

        with ThreadPoolExecutor(max_workers=len(driver.nodes)) as executor:

            futures = list()
            for node_id in driver.nodes:
                futures.append(
                    executor.submit(
                        self._prepare_query_on_worker,
                        node_id=node_id,
                        query=query,
                        datasets_metadata=datasets_metadata,
                        blocks_metadata=blocks_metadata,
                        partitions_metadata=partitions_metadata,
                        node_iterations_metadata=node_iterations_metadata[node_id],
                    )
                )

            for future in futures:
                _ = future.result()

    def _prepare_query_on_worker(
            self,
            node_id: str,
            query: Query,
            datasets_metadata: "pd.DataFrame",
            blocks_metadata: "pd.DataFrame",
            partitions_metadata: "pd.DataFrame",
            node_iterations_metadata: "pd.DataFrame",
    ) -> None:

        node_status = discovery_service.validate_node(node_id)

        query.workers[node_id] = WorkerStatus(
            node_id=node_id,
            state=QueryState.PREPARING_WORKERS,
        )

        query_params = query.params

        response = requests.post(
            url=f"{node_status.address}/worker/query/{query.query_id}/",
            files={
                "datasets_metadata": ("datasets", datasets_metadata.to_parquet(None, index=False)),
                "blocks_metadata": ("blocks", blocks_metadata.to_parquet(None, index=False)),
                "partitions_metadata": ("partitions", partitions_metadata.to_parquet(None, index=False)),
                "node_iterations_metadata": ("node_iterations", node_iterations_metadata.to_parquet(None, index=False)),
                "worker_kwargs": (None, json.dumps(query_params.worker_kwargs).encode("utf-8") if query_params.worker_kwargs else None),
                "partition_by": (None, json.dumps(query_params.partition_by).encode("utf-8")),
                "order_by": (None, json.dumps(query_params.order_by).encode("utf-8")),
            }
        )

        if response.status_code != 200:
            raise ValueError(f"Unable to create worker ({response.status_code}). Reason: {response.content}")

    def update_worker_status(
            self,
            query: Query,
            node_id: str,
            state: QueryState,
            detail: Optional[str] = None,
    ) -> None:

        worker_status = query.workers[node_id]

        if worker_status.state in QueryState.TERMINAL_STATES:
            return

        worker_status.state = state
        worker_status.detail = detail

        if state in (QueryState.ERROR, QueryState.TIMEOUT, QueryState.CANCELLED):
            query.state = state
            query.detail = detail

            for w in query.workers.values():
                if w.node_id == node_id:
                    continue
                w.state = QueryState.CANCELLED

            self.stop_query_on_workers(query)

            match state:
                case QueryState.ERROR:
                    message = {"error": detail}
                case QueryState.TIMEOUT:
                    message = {"timeout": True}
                case _:
                    message = {"cancelled": True}

            mq_service.send_msg_to_consumers(topic=query.query_id, msg=message)
            return

        states = [w.state for w in query.workers.values()]

        if all([v == QueryState.READY for v in states]):
            query.state = QueryState.RUNNING
            for r in query.workers.values():
                r.state = QueryState.RUNNING

            self.start_query_on_workers(query)

        elif all([v in (QueryState.COMPLETED, QueryState.FINISHED) for v in states]):
            query.state = QueryState.COMPLETED
            mq_service.send_msg_to_consumers(topic=query.query_id, msg={"finished": True})

        if all([v == QueryState.FINISHED for v in states]):
            query.state = QueryState.FINISHED
            query.finish_ts = datetime.now(timezone.utc)
            query.duration = (query.finish_ts - query.start_ts).total_seconds()
            # TODO: Remove MQ topic

    def stop_query_on_workers(self, query: Query):

        for node_id, worker in query.workers.items():

            if (node_status := discovery_service.nodes.get(node_id, None)) is None:
                continue

            if worker.state not in QueryState.TERMINAL_STATES:
                _ = requests.delete(f"{node_status.address}/worker/query/{query.query_id}")
                worker.state = QueryState.CANCELLED

        query.state = QueryState.CANCELLED

    @notify_clients_on_exception
    def start_query_on_workers(self, query: Query):
        for node_id in query.workers.keys():
            node_status = discovery_service.validate_node(node_id)

            response = requests.post(f"{node_status.address}/worker/query/{query.query_id}/start/")

            if response.status_code != 200:
                query.state = QueryState.ERROR
                detail = f"Query start error on worker {node_id}. Reason: {response.content}"
                query.detail = detail

                for worker in query.workers.values():
                    if worker.node_id == node_id:
                        worker.state = QueryState.ERROR
                        worker.detail = detail
                    else:
                        worker.state = QueryState.CANCELLED

                self.stop_query_on_workers(query)
                return

        query.state = QueryState.RUNNING


coordinator_query_service = CoordinatorQueryService()