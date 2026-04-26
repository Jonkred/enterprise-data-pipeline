.PHONY: help install install-dev test test-cov lint format clean docker-up docker-down data-init run-pipeline docs

help:
	@echo "Enterprise Data Pipeline - Available Commands"
	@echo "============================================"
	@echo "make install       - Install production dependencies"
	@echo "make install-dev   - Install with dev dependencies"
	@echo "make test          - Run tests"
	@echo "make test-cov      - Run tests with coverage"
	@echo "make lint          - Run linter (ruff)"
	@echo "make format        - Format code (ruff)"
	@echo "make type-check    - Run type checker (mypy)"
	@echo "make clean         - Clean build artifacts"
	@echo "make docker-up     - Start Docker services (Airflow, Postgres, Redis)"
	@echo "make docker-down   - Stop Docker services"
	@echo "make data-init     - Generate sample source data"
	@echo "make run-pipeline  - Execute full pipeline locally"
	@echo "make docs          - Generate documentation"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev,spark,airflow,kafka]"
	pre-commit install

test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html

lint:
	ruff check src/ tests/
	mypy src/

format:
	ruff format src/ tests/
	ruff check --fix src/ tests/

type-check:
	mypy src/

clean:
	rm -rf build/ dist/ *.egg-info/ .pytest_cache/ .mypy_cache/ htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down -v

data-init:
	python scripts/generate_sample_data.py

run-pipeline:
	python -m pipeline.cli run --config configs/pipeline.yaml

docs:
	@echo "Documentation available in docs/ directory"
	@echo "Run: make docs-serve to start local docs server"
