.PHONY: install up down produce consume api test test-integration lint fmt

install:
	pip install -e ".[dev]"

up:
	docker compose up -d

down:
	docker compose down -v

produce:
	python -m claims.producer --claims 25

consume:
	python -m claims.consumer

api:
	uvicorn claims.api:app --port 8000

test:
	pytest

test-integration:
	pytest -m integration

lint:
	ruff check .
	ruff format --check .

fmt:
	ruff check --fix .
	ruff format .
