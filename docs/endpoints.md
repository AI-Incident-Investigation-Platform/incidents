# HTTP API

Текущие маршруты определены в `src/api/*/router.py`. Ответы и поля следует
сверять с OpenAPI запущенного приложения (`/openapi.json`); продуктовый
целевой контракт описан в `backend/services/incidents.md`.

## Публичные маршруты

| Метод | Путь | Назначение |
|---|---|---|
| POST | `/api/v1/incidents` | Ручное создание |
| GET | `/api/v1/incidents` | Список |
| GET | `/api/v1/overview` | Счётчики проекта. `project_id` обязателен, `environment_id`, `from` и `to` необязательны |
| GET, PATCH | `/api/v1/incidents/{id}` | Карточка и правка; PATCH требует `If-Match` |
| PATCH | `/api/v1/incidents/{id}/status` | Resolve или Reopen |
| GET | `/api/v1/incidents/{id}/timeline` | Таймлайн |
| GET | `/api/v1/incidents/{id}/alerts` | Алерты |
| GET, POST | `/api/v1/incidents/{id}/reviews` | Список и добавление Review |
| GET, POST | `/api/v1/incidents/{id}/investigations` | Попытки и Start |
| GET | `/api/v1/investigations` | Список по проекту, статус и курсор |
| GET | `/api/v1/investigations/{id}` | Одна попытка |
| POST | `/api/v1/investigations/{id}/cancel` | Отмена |
| POST | `/api/v1/investigations/{id}/retry` | Новая попытка |
| GET | `/api/v1/investigations/{id}/events` | События после sequence |
| GET | `/api/v1/investigations/{id}/hypotheses` | Гипотезы |
| GET | `/api/v1/investigations/{id}/evidence` | Доказательства |
| GET | `/api/v1/investigations/{id}/report` | RCA при наличии итога |
| GET | `/api/v1/evidence/{id}` | Одно доказательство |
| GET | `/api/v1/ingest/receipts/{id}` | Статус квитанции ingest |

Публичные операции, кроме квитанций ingest, используют пользовательский
`Authorization: Bearer` и права проекта. Квитанция принимает также
`X-Ingest-Key`. Для Start и Retry нужен `Idempotency-Key`; детали ошибок
и параметров запроса смотрите в OpenAPI текущей версии.

## Внутренние маршруты

| Метод | Путь | Назначение |
|---|---|---|
| POST | `/internal/v1/ai/events` | Событие расследования от AI |
| POST | `/internal/v1/ai/results` | Терминальный результат от AI |
| GET | `/internal/v1/ai/incidents/{id}/context` | Инцидент, связанные сервисы и выкатки в окне от −30 до +5 минут от `detected_at`. Нужен токен `ai-backend` с правом `incidents.ai.context` |
| GET | `/internal/v1/investigations/{id}/report` | Данные отчёта для content-pdf |

Внутренние маршруты требуют служебный bearer-токен. Они доступны только
через кластерный Service; Ingress их не публикует.

## Служебные маршруты

`GET /health` — ответ процесса. `GET /ready` — PostgreSQL и Dragonfly.
Встроенные FastAPI `/docs` и `/openapi.json` доступны при прямом обращении
к Service, но не маршрутизируются Ingress.

`/overview` проверяет `project.read`, принимает bearer или cookie
`access_token` и возвращает `503`, если топология projects недоступна.
AI context для инцидента со связанными сервисами возвращает `503`, пока
не подключён источник каталога и выкаток. Для инцидента без связей
списки сервисов и выкаток пусты.
