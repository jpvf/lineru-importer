from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.aurora.twingate import start_monitor, stop_monitor
from app.notifications.bot import start_bot, stop_bot
from app.sync import worker
from app.state import repository as repo
from app.routes import tables, jobs, progress, settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    # On startup: resume any interrupted job, start Twingate monitor
    interrupted = repo.get_current_job()
    if interrupted and interrupted["status"] == "running":
        repo.update_job(interrupted["id"], status="paused")

    def on_twingate_failure():
        current = repo.get_current_job()
        if current and current["status"] == "running":
            worker.pause()
            repo.update_job(current["id"], status="paused")

    start_monitor(on_failure=on_twingate_failure)
    start_bot()
    yield
    stop_bot()
    stop_monitor()


app = FastAPI(title="aurora-sync", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(tables.router)
app.include_router(jobs.router)
app.include_router(progress.router)
app.include_router(settings.router)


@app.get("/api/health")
def health():
    return {"status": "ok"}
