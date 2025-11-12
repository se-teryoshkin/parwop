import os
print(os.environ)

from fastapi import FastAPI, APIRouter
from parwop.settings import IS_COORDINATOR, IS_WORKER
from parwop.utils import trim_memory


app = FastAPI()
root_router = APIRouter()


if IS_COORDINATOR:
    from parwop.server.coordinator.query.api import router as coordinator_query_router
    from parwop.server.coordinator.heartbeat.api import router as heartbeat_router, check_heartbeats

    app.include_router(coordinator_query_router, prefix="/coordinator/query")
    app.include_router(heartbeat_router, prefix="/coordinator/heartbeat")

    @app.on_event("startup")
    async def listen_heartbeats():
        await check_heartbeats()

if IS_WORKER:
    from parwop.server.worker.query.api import router as worker_query_router, check_if_queries_timed_out
    from parwop.server.worker.heartbeat.heartbeat import heartbeat
    from parwop.server.worker.statistic.api import router as statistics_router

    app.include_router(worker_query_router, prefix="/worker/query")
    app.include_router(statistics_router, prefix="/worker/statistic")

    @app.on_event("startup")
    async def heartbeat_send():
        await heartbeat()

    @app.on_event("startup")
    async def check_timeout():
        await check_if_queries_timed_out()

@root_router.put("/trim_memory")
def trim_mem() -> int:
    return trim_memory()


app.include_router(root_router)

