.PHONY: help lint format check test clean install-dev fix-whitespace

help:
	@echo "Available commands:"
	@echo "  install-dev    Install development dependencies"
	@echo "  lint          Run all linting checks"
	@echo "  format        Format code with black and isort"
	@echo "  check         Run linting without making changes"
	@echo "  fix-whitespace Remove trailing whitespace"
	@echo "  test          Run tests"
	@echo "  clean         Clean up temporary files"

install-dev:
	pip install -e ".[dev]"

lint: fix-whitespace format check

format:
	@echo "Formatting code with black..."
	black .
	@echo "Sorting imports with isort..."
	isort .

check:
	@echo "Checking code with flake8..."
	flake8 .
	@echo "Type checking with mypy..."
	mypy . --ignore-missing-imports

fix-whitespace:
	@echo "Removing trailing whitespace..."
	find . -name "*.py" -type f -exec sed -i 's/[[:space:]]*$$//' {} \;

test:
	pytest

clean:
	@echo "Cleaning up temporary files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true 