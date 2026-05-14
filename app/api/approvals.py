from fastapi import APIRouter, HTTPException

from app.workflows.graph import graph

from app.services.approval_service import (
    get_workflow,
    list_pending_workflows,
)

from app.workflows.nodes import (
    finalize_approval_node,
)

router = APIRouter(
    prefix="/approvals",
    tags=["approvals"],
)


@router.get("/pending")
async def pending_workflows():

    return list_pending_workflows()


@router.get("/{workflow_id}")
async def workflow_status(
    workflow_id: str,
):

    workflow = get_workflow(workflow_id)

    if not workflow:
        raise HTTPException(
            status_code=404,
            detail="Workflow not found",
        )

    return workflow


@router.post("/{workflow_id}/approve")
async def approve_workflow(
    workflow_id: str,
):

    workflow = get_workflow(workflow_id)

    if not workflow:
        raise HTTPException(
            status_code=404,
            detail="Workflow not found",
        )

    result = await finalize_approval_node(
        {
            **workflow,
            "approved": True,
        }
    )

    return {
        "workflow_id": workflow_id,
        "status": "APPROVED",
        "result": result,
    }


@router.post("/{workflow_id}/reject")
async def reject_workflow(
    workflow_id: str,
):

    workflow = get_workflow(workflow_id)

    if not workflow:
        raise HTTPException(
            status_code=404,
            detail="Workflow not found",
        )

    result = await finalize_approval_node(
        {
            **workflow,
            "approved": False,
        }
    )

    return {
        "workflow_id": workflow_id,
        "status": "REJECTED",
        "result": result,
    }