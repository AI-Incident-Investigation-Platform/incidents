.PHONY: help check lint-fix test test-integration test-all migrate upgrade run build secret migrate-job deploy up

IMAGE ?= aiip-incidents:dev
CONTEXT ?= k3d-aiip
NAMESPACE ?= apps
K8S_ENV_FILE ?=
KUBECTL := kubectl --context $(CONTEXT) -n $(NAMESPACE)

help:
	@echo "run, check, lint-fix, test, test-integration, test-all, migrate MSG=..., upgrade"
	@echo "build, secret, migrate-job, deploy, up (k3d-aiip, namespace apps)"
	@echo "Copy .env.example to .env for local use; set K8S_ENV_FILE for cluster credentials."

check:
	uv run ruff format --check src tests
	uv run ruff check src tests
	uv run mypy src tests

lint-fix:
	uv run ruff format src tests
	uv run ruff check --fix src tests

test:
	uv run pytest tests/unit

test-integration:
	uv run pytest tests/integration

test-all:
	uv run pytest tests

migrate:
	@test -n "$(MSG)" || { echo "Use make migrate MSG=description"; exit 1; }
	@if test -f .env; then uv run --env-file .env alembic revision --autogenerate -m "$(MSG)"; else test -n "$$DATABASE_URL" || { echo "Set DATABASE_URL or copy .env.example to .env"; exit 1; }; uv run alembic revision --autogenerate -m "$(MSG)"; fi

upgrade:
	@if test -f .env; then uv run --env-file .env alembic upgrade head; else test -n "$$DATABASE_URL" || { echo "Set DATABASE_URL or copy .env.example to .env"; exit 1; }; uv run alembic upgrade head; fi

run:
	@test -f .env || { echo "Copy .env.example to .env and set credentials"; exit 1; }
	uv run --env-file .env python -m src

build:
	docker build -t $(IMAGE) .
	k3d image import $(IMAGE) -c aiip

secret:
	@test -n "$(K8S_ENV_FILE)" && test -f "$(K8S_ENV_FILE)" || { echo "Set K8S_ENV_FILE to a populated cluster env file"; exit 1; }
	$(KUBECTL) create secret generic incidents --from-env-file="$(K8S_ENV_FILE)" --dry-run=client -o yaml | $(KUBECTL) apply -f -

migrate-job:
	$(KUBECTL) delete job incidents-migrate --ignore-not-found
	sed 's|aiip-incidents:dev|$(IMAGE)|' k8s/migrate.yaml | $(KUBECTL) apply -f -
	$(KUBECTL) wait --for=condition=complete job/incidents-migrate --timeout=180s

deploy:
	kubectl kustomize k8s | sed 's|aiip-incidents:dev|$(IMAGE)|' | $(KUBECTL) apply -f -
	$(KUBECTL) rollout status deployment/incidents --timeout=180s

up: build secret migrate-job deploy
