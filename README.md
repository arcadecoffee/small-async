# Async Worker Service

A lightweight, zero-dependency Python library for orchestrating concurrent, resilient asynchronous workers.

## Features

- 🛡️ **Graceful Shutdown**: Intercepts `SIGINT` and `SIGTERM` to allow workers to finish their current task.
- 🔄 **Resilient**: Automatically restarts workers if they encounter unhandled exceptions.
- 🏗️ **Isolated**: Uses the Factory pattern to ensure each worker "slot" has its own instance/state.
- 🧬 **Lifecycle Aware**: Includes an `asynccontextmanager` to ensure clean exit even during app crashes.

## Installation

This module is designed to be lightweight. You can copy `small_async.py` into your project or install it (if published).

```bash
uv add ./path/to/small_async
```
