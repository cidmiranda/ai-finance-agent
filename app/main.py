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

app = FastAPI()

app.include_router(
    approvals_router,
)


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