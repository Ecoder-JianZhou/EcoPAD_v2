import asyncio

from services.dispatcher import dispatch_run
from services.run_manager import update_run_status
from services.site_registry import get_site


class JobQueue:

    def __init__(self):

        self.queue = asyncio.Queue()

    async def worker(self):
        while True:
            job = await self.queue.get()
            try:
                await self.process(job)
            finally:
                self.queue.task_done()

    async def process(self, job):
        run_id = job["run_id"]
        site_id = job["site_id"]
        site = get_site(site_id)
        await update_run_status(run_id, "running")
        result = await dispatch_run(site["base_url"], job)
        status = result.get("status", "failed")
        await update_run_status(run_id, status)


job_queue = JobQueue()