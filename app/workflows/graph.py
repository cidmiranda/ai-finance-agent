from langgraph.graph import StateGraph
from app.workflows.state import WorkflowState
from app.workflows.nodes import reconcile_node

builder = StateGraph(WorkflowState)

builder.add_node("reconcile", reconcile_node)

builder.set_entry_point("reconcile")

builder.set_finish_point("reconcile")

graph = builder.compile()