from fastapi import APIRouter, Body, BackgroundTasks, HTTPException
from datetime import datetime, timezone
from typing import Optional

from parwop.server.common.models import QueryState, QueryParams
from parwop.server.common.exceptions import QueryAlreadyExists
from parwop.server.coordinator.query.service import Query, coordinator_query_service


router = APIRouter()


@router.get("/")
def list_queries() -> dict[str, Query]:
    return coordinator_query_service.get_queries()


@router.get("/{query_id}/")
def get_query(query_id: str) -> Query:

    query = coordinator_query_service.get_query(query_id)

    if query is None:
        raise HTTPException(status_code=404, detail=f"Query {query_id} not found")

    return query


@router.post("/{query_id}/")
def run_query(
        background: BackgroundTasks,
        query_id: str,
        query_params: QueryParams,
) -> Query:
    print(f"Query {query_id} params: {query_params}.")

    if coordinator_query_service.get_query(query_id) is not None:
        raise HTTPException(status_code=422, detail=f"Query {query_id} already exists.")

    query = Query(
        query_id=query_id,
        state=QueryState.PREPARING_COORDINATOR,
        workers=dict(),
        start_ts=datetime.now(timezone.utc),
        params=query_params,
    )

    try:
        coordinator_query_service.put_query(query)
    except QueryAlreadyExists:
        raise HTTPException(status_code=422, detail=f"Query {query_id} already exists.")

    background.add_task(coordinator_query_service.run_query, query=query)

    return query


@router.put("/{query_id}/worker_status")
def update_worker_status(
        background: BackgroundTasks,
        query_id: str,
        node_id: str = Body(...),
        state: QueryState = Body(...),
        detail: Optional[str] = Body(None)
):

    query = coordinator_query_service.get_query(query_id)

    print(f"WorkerStatusUpdate: node_id={node_id}, state={state}, query_id={query_id}")

    if query_id is None:
        raise HTTPException(status_code=404, detail=f"Query {query_id} not found")

    if node_id not in query.workers:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not assigned to query {query_id}")

    background.add_task(coordinator_query_service.update_worker_status, query, node_id, state, detail)


@router.delete("/{query_id}")
def stop_query(query_id: str, background: BackgroundTasks):

    query = coordinator_query_service.get_query(query_id)

    if query_id is None:
        raise HTTPException(status_code=404, detail="Query not found")

    if query.state in QueryState.TERMINAL_STATES:
        raise HTTPException(status_code=422, detail="Query already finished")

    background.add_task(coordinator_query_service.stop_query_on_workers, query)
