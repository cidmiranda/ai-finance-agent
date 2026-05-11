from langgraph.graph import StateGraph

from app.workflows.state import WorkflowState
from app.workflows.nodes import (
    reconciliation_agent
)

builder = StateGraph(WorkflowState)

builder.add_node(
    "reconciliation_agent",
    reconciliation_agent
)

builder.set_entry_point(
    "reconciliation_agent"
)

builder.set_finish_point(
    "reconciliation_agent"
)

graph = builder.compile()