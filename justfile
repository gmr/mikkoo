# List available recipes
default:
    @just --list

# Set up development environment
setup:
    uv sync --group dev

# One-time local dev setup: sync deps and install pre-commit hooks
bootstrap: setup
    uv run pre-commit install --install-hooks

# Run tests with coverage
test:
    mkdir -p build
    uv run coverage run
    uv run coverage report
    uv run coverage xml

# Run linting and formatting checks
lint:
    uv run pre-commit run --all-files

# Build distribution
build:
    uv run python -m build

# Build documentation
docs:
    uv run mkdocs build --strict

# Serve documentation locally
docs-serve:
    uv run mkdocs serve
