import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from async_worker.async_worker import AsyncWorkerService


@pytest.mark.asyncio
async def test_worker_service_runs_workers():
    """Verify that the service calls the worker's run method."""
    # Mock the worker
    mock_worker = MagicMock()

    async def mock_run(worker_id):
        await asyncio.sleep(0.01)

    mock_worker.run = AsyncMock(side_effect=mock_run)

    # Create the service
    stop_event = asyncio.Event()
    service = AsyncWorkerService(worker_factory=lambda: mock_worker, stop_event=stop_event)

    # Run the service in a task so we can stop it
    run_task = asyncio.create_task(service.run(num_workers=2))

    # Give it a moment to start workers
    await asyncio.sleep(0.1)

    # Signal stop
    stop_event.set()
    await run_task

    # Check if run was called (at least once per worker)
    assert mock_worker.run.called
    assert mock_worker.run.call_count >= 2


@pytest.mark.asyncio
async def test_worker_service_graceful_shutdown():
    """Verify that the service waits for workers to finish during shutdown."""
    stop_event = asyncio.Event()
    worker_finished = False

    class SlowWorker:
        async def run(self, worker_id: int) -> None:
            nonlocal worker_finished
            await stop_event.wait()
            await asyncio.sleep(0.2)  # Simulate some cleanup work
            worker_finished = True

    service = AsyncWorkerService(
        worker_factory=SlowWorker,
        stop_event=stop_event,
        worker_shutdown_grace_period=1
    )

    run_task = asyncio.create_task(service.run(num_workers=1))

    # Allow worker to start
    await asyncio.sleep(0.1)

    # Trigger shutdown
    stop_event.set()
    await run_task

    assert worker_finished is True


@pytest.mark.asyncio
async def test_worker_error_restart():
    """Verify that workers restart if they encounter an exception."""
    call_count = 0
    stop_event = asyncio.Event()

    class FailsOnceWorker:
        async def run(self, worker_id: int) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("Oops!")
            # Wait for the stop signal so we don't loop infinitely
            await stop_event.wait()

    # Set a tiny restart delay for the test
    service = AsyncWorkerService(
        worker_factory=FailsOnceWorker,
        stop_event=stop_event,
        worker_restart_delay=0.01
    )

    run_task = asyncio.create_task(service.run(num_workers=1))

    # Give it time to fail once and restart
    await asyncio.sleep(0.1)

    stop_event.set()
    await run_task

    assert call_count >= 2


@pytest.mark.asyncio
async def test_worker_force_cancellation():
    """Verify that workers are canceled if they exceed the grace period."""
    stop_event = asyncio.Event()

    class StubbornWorker:
        async def run(self, worker_id: int) -> None:
            # This worker ignores the stop_event and sleeps forever
            await asyncio.sleep(10)

    service = AsyncWorkerService(
        worker_factory=StubbornWorker,
        stop_event=stop_event,
        worker_shutdown_grace_period=0.1  # Very short grace period
    )

    run_task = asyncio.create_task(service.run(num_workers=1))
    await asyncio.sleep(0.05)

    stop_event.set()
    # This should complete because the service will cancel the stubborn worker
    await run_task
    # If we reached here, the service successfully canceled the worker


@pytest.mark.asyncio
async def test_worker_cancelled_during_error_sleep():
    """Verify worker exits cleanly if canceled while waiting to restart."""
    stop_event = asyncio.Event()

    class FailAlwaysWorker:
        async def run(self, worker_id: int) -> None:
            raise RuntimeError("Persistent error")

    service = AsyncWorkerService(
        worker_factory=FailAlwaysWorker,
        stop_event=stop_event,
        worker_restart_delay=5.0  # Long delay
    )

    run_task = asyncio.create_task(service.run(num_workers=1))
    await asyncio.sleep(0.05) # Allow it to fail at once and enter wait_for

    stop_event.set()
    # The worker should wake up from wait_for immediately and exit
    await asyncio.wait_for(run_task, timeout=1.0)


def test_signal_handler_registration():
    """Verify signal handlers are attempted to be registered."""
    # We can't easily test signal delivery in pytest without complexity,
    # but we can verify the registration function runs without crashing.
    mock_worker = MagicMock()

    service = AsyncWorkerService(worker_factory=MagicMock)

    # Mock loop.add_signal_handler to verify it's called
    with MagicMock() as mock_loop:
        from unittest.mock import patch

        with patch("asyncio.get_running_loop", return_value=mock_loop):
            service._setup_signal_handler()

            # Verify it tried to register SIGINT and SIGTERM
            assert mock_loop.add_signal_handler.call_count == 2


@pytest.mark.asyncio
async def test_signal_handler_execution():
    """Verify that the signal handler actually sets the stop event and cleans up."""
    service = AsyncWorkerService(worker_factory=MagicMock)

    with patch("asyncio.get_running_loop") as mock_get_loop:
        mock_loop = MagicMock()
        mock_get_loop.return_value = mock_loop

        # This will capture the signal handler function passed to add_signal_handler
        service._setup_signal_handler()

        # Get the actual handler function that was registered
        # call_args[0][2] is the stop_signal_handler closure
        args, _ = mock_loop.add_signal_handler.call_args
        handler_func = args[1]

        # Execute the handler
        handler_func("SIGINT")
        assert service.stop_event.is_set()

        # Execute the handler for the second time (Force Exit)
        with patch("os._exit") as mock_exit:
            handler_func("SIGINT")
            mock_exit.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_worker_final_timeout_error():
    """Cover the case where workers don't even respond to cancellation."""
    stop_event = asyncio.Event()

    class ShieldedWorker:
        async def run(self, worker_id: int) -> None:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                # Maliciously ignore cancellation for a bit
                await asyncio.sleep(10)

    service = AsyncWorkerService(
        worker_factory=ShieldedWorker,
        stop_event=stop_event,
        worker_shutdown_grace_period=0.1
    )

    run_task = asyncio.create_task(service.run(num_workers=1))
    await asyncio.sleep(0.05)

    stop_event.set()

    # We use wait_for here just to make sure the test doesn't hang,
    # but the service should hit its own internal TimeoutError
    await asyncio.wait_for(run_task, timeout=1.0)
    # Check logs for "Some workers did not exit after cancellation."


def test_setup_signal_handler_unsupported_platform():
    """Cover the exception block when signals aren't supported."""
    service = AsyncWorkerService(worker_factory=MagicMock)

    with patch("asyncio.get_running_loop", side_effect=RuntimeError("No loop")):
        # This should log a warning and return instead of crashing
        service._setup_signal_handler()


@pytest.mark.asyncio
async def test_worker_break_on_error_wait_cancellation():
    """Specifically, trigger the break inside the error-wait exception block."""
    stop_event = asyncio.Event()

    class FailOnceWorker:
        async def run(self, worker_id: int) -> None:
            raise RuntimeError("Initial Failure")

    service = AsyncWorkerService(
        worker_factory=FailOnceWorker,
        stop_event=stop_event,
        worker_restart_delay=10.0  # Long wait
    )

    # Start the worker
    worker_task = asyncio.create_task(service._start_worker(1))
    await asyncio.sleep(0.05)  # Let it hit the exception and start wait_for

    # Set the event AND cancel the task to hit the specific except branch
    stop_event.set()
    worker_task.cancel()

    try:
        await worker_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_async_context_manager():
    """Verify the service works as an async context manager."""
    service = AsyncWorkerService(worker_factory=MagicMock)

    async with service.lifecycle() as worker_service:
        assert worker_service == service
        assert not worker_service.stop_event.is_set()

    # Exiting the context should set the stop event
    assert service.stop_event.is_set()


@pytest.mark.asyncio
async def test_async_context_manager_already_stopped():
    """Verify exiting context doesn't interfere if stop_event is already set."""
    service = AsyncWorkerService(worker_factory=MagicMock)

    async with service.lifecycle() as worker_service:
        worker_service.stop_event.set()

    assert service.stop_event.is_set()
