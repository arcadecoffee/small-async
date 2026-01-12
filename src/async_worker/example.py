import asyncio
import logging
from functools import partial

from async_worker import AsyncWorkerService

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class MyTask:
    worker_sleep_interval: int = 5
    cycles: int = 0  # For testing purposes

    async def run(self, worker_id: int) -> None:
        logger.info(f"Worker {worker_id} working...")
        self.cycles += 1
        logger.info(f"Worker {worker_id} cycle: {self.cycles}")
        await asyncio.sleep(self.worker_sleep_interval)


async def main_async():
    # Use partial to pass configuration to the worker constructor
    factory = partial(MyTask)

    # Initialize the service
    service = AsyncWorkerService(
        worker_factory=factory,
        worker_restart_delay=1.0,
        worker_shutdown_grace_period=10.0   # Should be longer than worker_sleep_interval
    )

    # Use the lifecycle context manager for safety
    async with service.lifecycle() as worker_service:
        print("Starting workers. Press Ctrl+C to stop.")
        await worker_service.run(num_workers=3)


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
