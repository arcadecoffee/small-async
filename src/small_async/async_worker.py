import asyncio
import logging
import signal

from contextlib import asynccontextmanager
from typing import Callable, Protocol, Self

from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


class AsyncWorker(Protocol):
    """
    Protocol defining the requirements for a worker instance.
    Implementations must provide an asynchronous 'run' method.
    Attributes:
        worker_id: An integer identifier for the worker instance.
    """
    async def run(self, worker_id: int) -> None:
        ...


@dataclass
class AsyncWorkerService:
    """
    Service to manage multiple asynchronous workers with graceful shutdown and error handling.

    Attributes:
        worker_factory: A callable that returns a new instance of an AsyncWorker.
        worker_shutdown_grace_period: Time in seconds to wait for workers to finish during shutdown.
        worker_restart_delay: Time in seconds to wait before restarting a worker after an error.
        stop_event: An asyncio.Event used to signal workers to stop.
    """
    worker_factory: Callable[[], AsyncWorker]
    worker_shutdown_grace_period: float = 30.0
    worker_restart_delay: float = 2.0
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)

    def _setup_signal_handler(self):
        try:
            loop = asyncio.get_running_loop()

            def stop_signal_handler(signal_name: str) -> None:
                if self.stop_event.is_set():
                    logger.warning(f"Forcing exit...")
                    # Use os._exit to bypass any remaining cleanup and exit immediately
                    import os
                    os._exit(1)

                logger.warning(f"Shutdown signal {signal_name} received. Signaling workers to stop...")
                self.stop_event.set()
                logger.warning(f"Press CTRL+C again to force exit.")

            for sig in (signal.SIGINT, signal.SIGTERM):
                logger.info(f"Registering signal handler for {signal.Signals(sig).name}")
                loop.add_signal_handler(sig, stop_signal_handler, signal.Signals(sig).name)
        except (ValueError, NotImplementedError, RuntimeError):
            logger.warning("Could not register signal handlers (expected in tests or non-main thread).")

    async def _start_worker(self, worker_id: int) -> None:
        worker = self.worker_factory()
        logger.info(f"Worker {worker_id} starting...")
        while not self.stop_event.is_set():
            try:
                await worker.run(worker_id)
            except asyncio.CancelledError:
                logger.info(f"Worker {worker_id} cancellation requested.")
                return
            except Exception:
                logger.exception(f"Worker {worker_id} encountered an error")
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=self.worker_restart_delay)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    if self.stop_event.is_set():
                        break
                    logger.info(f"Reinitializing worker {worker_id} after error...")
                    worker = self.worker_factory()

        logger.info(f"Worker {worker_id} shut down cleanly.")

    async def run(self, num_workers: int = 5):
        """
        Start the specified number of workers and manage their lifecycle.
        Workers will be restarted if they encounter an error, with a delay between restarts.
        The service will wait for workers to finish during shutdown, with a grace period.
        Args:
            num_workers: The number of worker instances to start.
        """
        self._setup_signal_handler()
        logger.info(f"Starting {num_workers} workers...")
        tasks = [asyncio.create_task(self._start_worker(worker_id)) for worker_id in range(num_workers)]

        await self.stop_event.wait()

        logger.warning(f"Shutdown signal received. Allowing {self.worker_shutdown_grace_period} seconds for workers to finish...")

        _, pending = await asyncio.wait(tasks, timeout=self.worker_shutdown_grace_period)

        if pending:
            logger.warning(f"{len(pending)} workers did not exit in time. Asking them to cancel...")
            for task in pending:
                task.cancel()
            logger.warning("Waiting for cancelled workers to finish...")

            try:
                await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=self.worker_shutdown_grace_period)
            except asyncio.TimeoutError:
                logger.error("Some workers did not exit after cancellation.")

        logger.info("All workers have stopped.")

    @asynccontextmanager
    async def lifecycle(self) -> Self:
        """
        Context manager to manage the lifecycle of the worker service.
        Ensures that workers are properly started and stopped, with graceful shutdown and error handling.
        """
        try:
            yield self
        finally:
            if not self.stop_event.is_set():
                self.stop_event.set()
