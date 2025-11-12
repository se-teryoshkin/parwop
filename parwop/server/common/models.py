from typing import Union, Optional
from pydantic import BaseModel, Field
from fastapi import HTTPException
from enum import Enum

from parwop.solver.dataset import DATASET_TYPES


class QueryState(str, Enum):
    PREPARING_COORDINATOR = "Preparing coordinator"
    """
    Preparing coordinator -> Plan building / Error. 
    Set by driver on receive the query request.
    """

    PLAN_BUILDING = "Plan building"
    """
    Plan building -> Preparing workers / Error.
    Set by driver while it prepare reading plans.
    """

    PREPARING_WORKERS = "Preparing workers"
    """
    Preparing workers -> Loading / Error. 
    Set by driver send request to the worker to prepare the query execution.
    """

    LOADING = "Loading"
    """
    Loading -> Ready / Error.
    Set only on worker side (do not report the driver on this state) on receive the request from the driver. 
    Worker loads metadata from the request.
    """

    READY = "Ready"
    """
    Ready -> Running / Error / Cancelled.
    Set by worker. Worker is ready to start query, but waiting for the command from the driver.
    """

    RUNNING = "Running"
    """
    Running -> Completed / Error / Cancelled / Timeout.
    Set by worker. This status lasts while query is running.
    """

    COMPLETED = "Completed"
    """
    Completed -> Finished / Cancelled.
    Set by worker. All the data have been read and processed, but the data aren't consumed by the client yet. 
    """

    FINISHED = "Finished"
    """
    Terminal state.
    Set by worker when all the data were consumed by clients.
    """

    CANCELLED = "Cancelled"
    """
    Terminal state.
    Set by driver on query cancelling.
    """

    TIMEOUT = "Timeout"
    """
    Terminal state.
    Set by worker when the query running is reached the timeout.
    """

    ERROR = "Error"
    """
    Terminal state.
    Set by worker on any unexpected error.
    """

    TERMINAL_STATES = {FINISHED, CANCELLED, TIMEOUT, ERROR}
    RUNNING_STATES = {LOADING, COMPLETED, RUNNING}


class DatasetRequest(BaseModel):
    dataset_type: str
    dataset_kwargs: dict[str, Union[float, int, str]]

    def to_object(self):

        dataset_type = self.dataset_type
        if dataset_type not in DATASET_TYPES:
            raise HTTPException(status_code=422, detail=f"Invalid dataset type: {dataset_type}")

        return DATASET_TYPES[dataset_type](**self.dataset_kwargs)


class QueryParams(BaseModel):
    partition_by: list[str]
    order_by: list[str]
    datasets: list[DatasetRequest]
    memory_limit_records: Optional[int] = Field(None)
    solver_kwargs: Optional[dict] = Field(None)
    worker_kwargs: Optional[dict] = Field(None)
