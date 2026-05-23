# DeepTutor Development Makefile
# ============================================
# Quick reference:
#   make dev          Start development environment (4 containers)
#   make prod         Start production stack
#   make test         Run all tests
#   make lint         Run pre-commit checks on all files
#   make logs         Tail container logs
#   make shell        Open a shell in the deeptutor container
#   make clean        Remove temporary files and caches
# ============================================

SHELL := /bin/bash

.PHONY: dev prod test lint logs shell clean setup

# --- Environment ---

dev:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build

prod:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

shell:
	docker exec -it deeptutor bash

# --- Testing ---

test:
	python -m pytest tests/ -x -q

test-v:
	python -m pytest tests/ -x -v

test-coverage:
	python -m pytest tests/ --cov=deeptutor --cov=tutor_platform --cov=domains -x

# --- Code Quality ---

lint:
	pre-commit run --all-files

lint-ruff:
	ruff check --fix .

lint-ruff-format:
	ruff format .

# --- Setup ---

setup:
	pip install pre-commit
	pre-commit install

# --- Cleanup ---

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .coverage htmlcov/
