# Telegram Subscription Bot — Supabase/Postgres Edition (Render Free)

- Персистентность через внешнюю БД (Supabase Postgres).
- На Render Free, без дисков.
- Robokassa, мини-админка, логи, напоминания, /stats, /health.

## Быстрый запуск
1) Создай проект в Supabase → скопируй Connection String (Database URL, `?sslmode=require`).
2) Заполни переменные в Render → Environment (см. .env.example), особенно `DATABASE_URL`.
3) Скопируй `.env.example` в `.env` и заполни переменные, особенно `DATABASE_URL` (или настрой их в Render → Environment)
4) Деплой по render.yaml (Blueprint).
## Переменные окружения
Скопируй `.env.example` в `.env` и заполни:

- `BOT_TOKEN`
- `BASE_URL`
- `WEBHOOK_SECRET`
- `CHANNEL_ID`
- `ADMIN_USER_ID`
- `ROBOKASSA_LOGIN`
- `ROBOKASSA_PASSWORD1`
- `ROBOKASSA_PASSWORD2`
- `ROBOKASSA_SIGNATURE_ALG`
- `ROBOKASSA_TEST_MODE`
- `PRICE_RUB`
- `SUBSCRIPTION_DAYS`
- `DATABASE_URL` (или `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `PROJECT_REF`)

## Схема БД
Таблицы `users`, `payments`, `logs` создаются автоматически при старте (init_db).
`logs` хранит `id`, `created_at`, `tg_id`, `message` и индекс по `tg_id`.
