import aiosqlite

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.graph import StateGraph, END

from app.workflows.state import (
    WorkflowState,
)

from app.workflows.nodes import (
    reconciliation_agent,
    human_approval_node,
    wait_for_approval_node,
    auto_approve_node,
)

from app.workflows.routes import (
    route_after_reconciliation,
)

# =========================================================
# SQLITE ASYNC CHECKPOINTER
# =========================================================

sqlite_connection = aiosqlite.connect(
    "checkpoints.db"
)

checkpointer = AsyncSqliteSaver(
    sqlite_connection
)

print("CHECKPOINTER TYPE:", type(checkpointer))

# =========================================================
# WORKFLOW
# =========================================================

workflow = StateGraph(
    WorkflowState,
)

workflow.add_node(
    "reconciliation_agent",
    reconciliation_agent,
)

workflow.add_node(
    "human_approval",
    human_approval_node,
)

workflow.add_node(
    "wait_for_approval",
    wait_for_approval_node,
)

workflow.add_node(
    "auto_approve",
    auto_approve_node,
)

workflow.set_entry_point(
    "reconciliation_agent",
)

workflow.add_conditional_edges(
    "reconciliation_agent",
    route_after_reconciliation,
    {
        "human_approval": "human_approval",
        "auto_approve": "auto_approve",
    },
)

workflow.add_edge(
    "human_approval",
    "wait_for_approval",
)

workflow.add_edge(
    "wait_for_approval",
    END,
)

workflow.add_edge(
    "auto_approve",
    END,
)

# =========================================================
# COMPILE GRAPH
# =========================================================

graph = workflow.compile(
    checkpointer=checkpointer,
)