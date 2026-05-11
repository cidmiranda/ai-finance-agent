from fastapi import FastAPI
from app.workflows.graph import graph

app = FastAPI()

@app.post("/reconcile")
async def reconcile():

    result = await graph.ainvoke({
        "exchange_balance": 10000,
        "blockchain_balance": 9700
    })

    return result