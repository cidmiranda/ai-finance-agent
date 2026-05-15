from app.storage.workflow_store import (
    workflow_store,
)


def create_workflow(state: dict):

    workflow_id = state["workflow_id"]

    workflow_store[workflow_id] = state

    return workflow_id



def update_workflow(
    workflow_id: str,
    data: dict,
):
    workflow_store[workflow_id].update(data)



def get_workflow(
    workflow_id: str,
):

    return workflow_store.get(workflow_id)



def list_pending_workflows():

    return [
        workflow
        for workflow in workflow_store.values()
        if workflow.get("status") == "WAITING_APPROVAL"
    ]