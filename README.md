# incidents

Сервис AIIP хранит инциденты, алерты, расследования, RCA и Review. Процесс
`python -m src` поднимает HTTP API, консьюмеры RabbitMQ, outbox relay и
диспетчер расследований. Схема БД обновляется Alembic до запуска процесса.

## Локальный запуск

Нужны Python 3.14, `uv`, PostgreSQL, RabbitMQ, Dragonfly и доступ к `auth`,
`projects`, AI Backend. Скопируйте `.env.example` в `.env`, укажите реальные
адреса и секреты, затем:

```bash
make upgrade
make run
```

`make check` запускает Ruff и mypy; `make test`, `make test-integration` и
`make test-all` запускают тесты. `make migrate MSG="описание"` создаёт
автомиграцию после проверки моделей и текущих heads. Порт по умолчанию 8006.
`GET /health` отвечает, когда жив HTTP процесс; `GET /ready` проверяет
PostgreSQL и Dragonfly. Во время старта процесс также подключается к
RabbitMQ: без брокера API не поднимется. `/ready` не проверяет брокер или
соседние HTTP сервисы.

## k3d

Образ и манифесты рассчитаны на кластер `aiip`, контекст `k3d-aiip` и
namespace `apps`. Подготовьте отдельный env файл с адресами PostgreSQL,
RabbitMQ и Dragonfly, доступными **из кластера**, и передайте его через
`K8S_ENV_FILE`. Значения `localhost` из `.env.example` подходят только
для запуска на хосте. Секрет `incidents` создаётся из этого файла;
`AUTH_URL`, `PROJECTS_URL` и `AI_BACKEND_URL` заданы в Deployment через
DNS сервисов, при необходимости измените манифест.

```bash
make up K8S_ENV_FILE=/path/to/incidents-k8s.env IMAGE=aiip-incidents:dev
```

`make up` собирает и импортирует образ, обновляет Secret, выполняет Job
миграции, затем применяет Deployment, Service и Traefik Ingress.
Шаги доступны отдельно как `make build`, `make secret`, `make migrate-job`,
`make deploy`. Миграция в Job имеет отдельный жизненный цикл и не входит
в `kustomization.yaml`. Для повторного `make migrate-job` предыдущий Job
удаляется. Тег образа и секрет задавайте для конкретной среды; секреты
в git не храните.

Ingress маршрутизирует только публичные пути `/api/v1/incidents`,
`/api/v1/investigations`, `/api/v1/evidence` и
`/api/v1/ingest/receipts`. Внутренние callbacks и чтение отчёта доступны
через `http://incidents.apps:8006`, без внешнего маршрута. Кластерную
готовность и реальную доставку сообщений надо проверить в целевой среде.

## Документация

- [Эндпоинты](docs/endpoints.md)
- [Устройство и зависимости](docs/design.md)
- [Требования и оставшаяся работа](docs/requirements.md)

Основной продуктовый контракт: [`backend/services/incidents.md`](../backend/services/incidents.md),
топология сообщений: [`backend/messaging.md`](../backend/messaging.md).
