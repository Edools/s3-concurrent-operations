.PHONY: help lint format check clean install-dev install update-pip

help:
	@echo "Available commands:"
	@echo "  install-dev    Install development dependencies"
	@echo "  lint          Run all linting checks"
	@echo "  format        Format code with black and isort"
	@echo "  check         Run linting without making changes"
	@echo "  clean         Clean up temporary files"

update-pip:
	python -m pip install --upgrade pip

install-dev:
	python -m pip install -r requirements-dev.txt

install:
	python -m pip install -r requirements.txt

lint: format check

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

clean:
	@echo "Cleaning up temporary files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true 