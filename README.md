# Telegram Subscription Bot — Supabase/Postgres Edition (Render Free)

- Персистентность через внешнюю БД (Supabase Postgres).
- На Render Free, без дисков.
- Robokassa, мини-админка, логи, напоминания, /stats, /health.

## Быстрый запуск
1) Создай проект в Supabase → скопируй Connection String (Database URL, `?sslmode=require`).
2) Заполни переменные в Render → Environment (см. .env.example), особенно `DATABASE_URL`.
3) Скопируй `.env.example` в `.env` и заполни переменные, особенно `DATABASE_URL` (или настрой их в Render → Environment)
4) Деплой по render.yaml (Blueprint).

## Схема БД
Таблицы `users`, `payments`, `logs` создаются автоматически при старте (init_db).
