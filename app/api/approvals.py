from fastapi import APIRouter, HTTPException

from langgraph.types import Command

from app.workflows.graph import graph

from app.services.approval_service import (
    get_workflow,
    list_pending_workflows,
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

    config = {"configurable": {"thread_id": workflow_id}}

    result = await graph.ainvoke(
        Command(resume={"approved": True}),
        config=config,
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

    config = {"configurable": {"thread_id": workflow_id}}

    result = await graph.ainvoke(
        Command(resume={"approved": False}),
        config=config,
    )

    return {
        "workflow_id": workflow_id,
        "status": "REJECTED",
        "result": result,
    }