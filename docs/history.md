# History

## 3.0.0 (2026-03-25)

- Modernize build system to pyproject.toml with hatchling and uv
- Switch from flake8 to ruff for linting and formatting
- Add pre-commit hooks and justfile task runner
- Convert documentation to MkDocs with Material theme
- Add GitHub Actions docs deployment workflow
- Update CI to test Python 3.11, 3.12, 3.13, and 3.14
- Switch to trusted PyPI publishing
- Fix RabbitMQ channel reconnect race condition causing unrecoverable worker state
- Fix MCP process reference TOCTOU race condition in remove_worker_process
- Drop support for Python < 3.11

## 2.2.2 (2024-12-17)

- Address stability issues

## 2.2.1 (2024-12-13)

- Handle missing process and log it

## 2.2.0 (2024-12-13)

- Attempt to handle disconnects better

## 2.1.0 (2024-12-13)

- Fix authentication handling

## 2.0.0 (2024-12-13)

- Modernize codebase
- Switch from queries to psycopg3
- Update configuration format
