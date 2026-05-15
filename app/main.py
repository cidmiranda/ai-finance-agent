import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI

from app.workflows.graph import graph

from app.api.approvals import (
    router as approvals_router,
)

from app.services.approval_service import (
    create_workflow,
)

from app.schemas.reconciliation import (
    ReconciliationRequest,
)

from app.mcp_server.server import mcp

from app.kafka import producer as kafka_producer
from app.kafka import consumer as kafka_consumer
from app.kafka.topics import RECONCILIATION_REQUESTED
from app.telemetry.tracer import setup_telemetry
from temporalio.client import Client

from app.temporal.worker import run_worker, TASK_QUEUE
from app.temporal.workflows import ReconciliationSaga
from app.config.settings import TEMPORAL_HOST

mcp_asgi = mcp.streamable_http_app()


async def handle_reconciliation_from_kafka(payload: dict):
    workflow_id = str(uuid4())

    initial_state = {
        "workflow_id": workflow_id,
        "status": "RUNNING",
        "exchange_balance": payload.get("exchange_balance", 0),
        "blockchain_balance": payload.get("blockchain_balance", 0),
        "approved": False,
        "requires_approval": False,
    }

    create_workflow(initial_state)

    config = {"configurable": {"thread_id": workflow_id}}

    await graph.ainvoke(initial_state, config=config)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    async with mcp.session_manager.run():
        await kafka_producer.start()
        consumer_task = asyncio.create_task(
            kafka_consumer.start(handle_reconciliation_from_kafka)
        )
        worker_task = asyncio.create_task(run_worker())
        try:
            yield
        finally:
            consumer_task.cancel()
            worker_task.cancel()
            await kafka_producer.stop()


app = FastAPI(lifespan=lifespan)

setup_telemetry(app)

app.include_router(
    approvals_router,
)

app.mount("/mcp-server", mcp_asgi)


@app.post("/reconcile")
async def reconcile(
    request: ReconciliationRequest,
):

    workflow_id = str(uuid4())

    initial_state = {
        "workflow_id": workflow_id,
        "status": "RUNNING",

        "exchange_balance": request.exchange_balance,
        "blockchain_balance": request.blockchain_balance,

        "approved": False,
        "requires_approval": False,
    }

    print("INITIAL STATE:", initial_state)

    create_workflow(initial_state)

    config = {
        "configurable": {
            "thread_id": workflow_id,
        }
    }

    try:

        result = await graph.ainvoke(
            initial_state,
            config=config,
        )

        print("GRAPH RESULT:", result)

        return {
            "workflow_id": workflow_id,
            "result": result,
        }

    except Exception as e:

        print("RECONCILIATION ERROR:", repr(e))

        return {
            "workflow_id": workflow_id,
            "status": "ERROR",
            "error": str(e),
        }


@app.post("/reconcile/kafka")
async def reconcile_via_kafka(
    request: ReconciliationRequest,
):

    await kafka_producer.publish(
        RECONCILIATION_REQUESTED,
        {
            "exchange_balance": request.exchange_balance,
            "blockchain_balance": request.blockchain_balance,
        },
    )

    return {"status": "published", "topic": RECONCILIATION_REQUESTED}


@app.post("/reconcile/temporal")
async def reconcile_via_temporal():
    workflow_id = str(uuid4())
    client = await Client.connect(TEMPORAL_HOST)

    await client.start_workflow(
        ReconciliationSaga.run,
        workflow_id,
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )

    return {"workflow_id": workflow_id, "status": "started"}


@app.post("/reconcile/temporal/{workflow_id}/approve")
async def approve_temporal_workflow(workflow_id: str):
    client = await Client.connect(TEMPORAL_HOST)
    handle = client.get_workflow_handle(workflow_id)
    await handle.signal(ReconciliationSaga.receive_decision, True)

    return {"workflow_id": workflow_id, "status": "approved"}


@app.post("/reconcile/temporal/{workflow_id}/reject")
async def reject_temporal_workflow(workflow_id: str):
    client = await Client.connect(TEMPORAL_HOST)
    handle = client.get_workflow_handle(workflow_id)
    await handle.signal(ReconciliationSaga.receive_decision, False)

    return {"workflow_id": workflow_id, "status": "rejected"}