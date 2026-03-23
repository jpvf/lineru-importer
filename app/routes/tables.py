from fastapi import APIRouter, BackgroundTasks, Query
from app.aurora.discovery import discover_all
from app.state import repository as repo

router = APIRouter(prefix="/api/tables", tags=["tables"])


@router.get("")
def list_tables(
    search: str = Query(""),
    schema: str = Query(""),
    strategy: str = Query(""),
    only_selected: bool = Query(False),
):
    tables = repo.get_tables(search=search, schema=schema, strategy=strategy, only_selected=only_selected)
    total_bytes = sum(t["data_length_bytes"] or 0 for t in tables)
    selected_bytes = sum(t["data_length_bytes"] or 0 for t in tables if t["sync_data"])
    return {
        "tables": tables,
        "total": len(tables),
        "total_bytes": total_bytes,
        "selected_bytes": selected_bytes,
    }


@router.post("/discover")
def trigger_discovery(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_discovery)
    return {"status": "discovery_started"}


def _run_discovery():
    tables = discover_all()
    for t in tables:
        repo.upsert_table(t)


@router.post("/selection")
def bulk_selection(body: dict):
    """body: { "selections": [{"schema":"x","table":"y","sync_data":true}] }"""
    repo.bulk_set_selection(body.get("selections", []))
    return {"status": "ok"}


@router.patch("/{schema_name}/{table_name}")
def update_table(schema_name: str, table_name: str, body: dict):
    if "sync_data" in body:
        repo.set_table_selection(schema_name, table_name, body["sync_data"])
    return {"status": "ok"}


@router.get("/{schema_name}/{table_name}")
def get_table(schema_name: str, table_name: str):
    t = repo.get_table(schema_name, table_name)
    if not t:
        return {"error": "not found"}, 404
    s = repo.get_sync_state(schema_name, table_name)
    return {"table": t, "sync_state": s}
