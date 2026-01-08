import os
import hashlib
import asyncio
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, field

import uvicorn
import socketio
from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# =======================
# LOGGING
# =======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("CaptchaServer")

# =======================
# FASTAPI APP
# =======================
app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Static (mp3, js, css...)
app.mount("/static", StaticFiles(directory="static"), name="static")

# =======================
# SOCKET.IO
# =======================
sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*"
)

socket_app = socketio.ASGIApp(sio, app)

# =======================
# DATA STRUCTURES
# =======================

@dataclass
class Job:
    id: str
    image_data: bytes
    futures: List[asyncio.Future] = field(default_factory=list)
    created_at: float = field(default_factory=lambda: asyncio.get_event_loop().time())


class JobManager:
    def __init__(self):
        self.queue: asyncio.Queue[Job] = asyncio.Queue()
        self.active_jobs: Dict[str, Job] = {}
        self.current_processing_job: Optional[Job] = None
        self.lock = asyncio.Lock()

    async def add_job(self, image_bytes: bytes) -> Optional[str]:
        image_hash = hashlib.md5(image_bytes).hexdigest()

        loop = asyncio.get_running_loop()
        future = loop.create_future()

        async with self.lock:
            if image_hash in self.active_jobs:
                logger.info(f"♻️ Retry job {image_hash[:8]}")
                self.active_jobs[image_hash].futures.append(future)
            else:
                logger.info(f"🆕 New job {image_hash[:8]}")
                job = Job(id=image_hash, image_data=image_bytes, futures=[future])
                self.active_jobs[image_hash] = job
                await self.queue.put(job)

                await self.broadcast_state()

                if not self.current_processing_job:
                    await self.dispatch_next_job()

        try:
            result = await asyncio.wait_for(future, timeout=120)
            return result
        except asyncio.TimeoutError:
            logger.warning(f"⏱️ Job timeout {image_hash[:8]}")
            return None

    async def dispatch_next_job(self):
        if self.queue.empty():
            self.current_processing_job = None
            await self.broadcast_state()
            return

        job = await self.queue.get()
        self.current_processing_job = job

        await sio.emit("update_state", {
            "queue_size": self.queue.qsize(),
            "active_id": job.id,
            "active_url": f"/get_image/{job.id}"
        })

    async def solve_job(self, job_id: str, answer: str) -> bool:
        async with self.lock:
            if not self.current_processing_job:
                return False

            if self.current_processing_job.id != job_id:
                return False

            job = self.current_processing_job
            logger.info(f"✅ Solved {job_id[:8]} => {answer}")

            for f in job.futures:
                if not f.done():
                    f.set_result(answer)

            self.active_jobs.pop(job_id, None)
            self.current_processing_job = None

            await self.dispatch_next_job()
            return True

    async def broadcast_state(self):
        await sio.emit("update_state", {
            "queue_size": self.queue.qsize(),
            "active_id": self.current_processing_job.id if self.current_processing_job else None,
            "active_url": f"/get_image/{self.current_processing_job.id}"
            if self.current_processing_job else None
        })


manager = JobManager()

# =======================
# API ROUTES
# =======================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request}
    )


@app.post("/solve")
async def receive_captcha(file: UploadFile = File(...)):
    content = await file.read()
    result = await manager.add_job(content)

    if result is None:
        return JSONResponse(
            {"error": "Timeout or failed"},
            status_code=500
        )

    return {"number": result}


@app.post("/submit_answer")
async def submit_answer(request: Request):
    data = await request.json()
    job_id = data.get("id")
    answer = data.get("answer")

    if not job_id or answer is None:
        return {"status": "failed"}

    success = await manager.solve_job(job_id, answer)
    return {"status": "success" if success else "failed"}


@app.get("/get_image/{job_id}")
async def get_image(job_id: str):
    job = manager.active_jobs.get(job_id)
    if not job:
        return JSONResponse({"error": "Not found"}, status_code=404)

    return Response(
        content=job.image_data,
        media_type="image/png"
    )

# =======================
# SOCKET.IO EVENTS
# =======================

@sio.event
async def connect(sid, environ):
    logger.info(f"🌐 Web UI connected: {sid}")
    await manager.broadcast_state()


@sio.event
async def disconnect(sid):
    logger.info(f"❌ Web UI disconnected: {sid}")

# =======================
# ENTRY POINT
# =======================

if __name__ == "__main__":
    os.makedirs("static", exist_ok=True)

    port = int(os.environ.get("PORT", 5000))
    uvicorn.run(
        socket_app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
