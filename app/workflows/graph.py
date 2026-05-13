from langgraph.graph import (
    StateGraph,
    END
)

from app.workflows.state import (
    WorkflowState,
)

from app.workflows.nodes import (
    reconciliation_agent,
    human_approval_node,
    auto_approve_node
)

from app.workflows.routes import (
    route_after_reconciliation
)


workflow = StateGraph(
    WorkflowState
)

# nodes

workflow.add_node(
    "reconciliation_agent",
    reconciliation_agent
)

workflow.add_node(
    "human_approval",
    human_approval_node
)

workflow.add_node(
    "auto_approve",
    auto_approve_node
)

# entry point

workflow.set_entry_point(
    "reconciliation_agent"
)

# CONDITIONAL ROUTING

workflow.add_conditional_edges(
    "reconciliation_agent",

    route_after_reconciliation,

    {
        "human_approval":
            "human_approval",

        "auto_approve":
            "auto_approve"
    }
)

# finish flow

workflow.add_edge(
    "human_approval",
    END
)

workflow.add_edge(
    "auto_approve",
    END
)

graph = workflow.compile()