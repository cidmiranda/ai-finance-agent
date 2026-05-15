import aiosqlite

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.graph import StateGraph, END

from app.workflows.state import WorkflowState
from app.workflows.nodes import (
    multi_agent_reconciliation_node,
    human_approval_node,
    wait_for_approval_node,
    auto_approve_node,
)
from app.workflows.routes import route_after_reconciliation

sqlite_connection = aiosqlite.connect("checkpoints_multi_agent.db")
checkpointer = AsyncSqliteSaver(sqlite_connection)

workflow = StateGraph(WorkflowState)

workflow.add_node("multi_agent_reconciliation", multi_agent_reconciliation_node)
workflow.add_node("human_approval", human_approval_node)
workflow.add_node("wait_for_approval", wait_for_approval_node)
workflow.add_node("auto_approve", auto_approve_node)

workflow.set_entry_point("multi_agent_reconciliation")

workflow.add_conditional_edges(
    "multi_agent_reconciliation",
    route_after_reconciliation,
    {
        "human_approval": "human_approval",
        "auto_approve": "auto_approve",
    },
)

workflow.add_edge("human_approval", "wait_for_approval")
workflow.add_edge("wait_for_approval", END)
workflow.add_edge("auto_approve", END)

multi_agent_graph = workflow.compile(checkpointer=checkpointer)
