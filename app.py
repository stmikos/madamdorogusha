# app.py
# -*- coding: utf-8 -*-
from textwrap import dedent
import os, re, asyncio, logging, secrets, hashlib
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery, Update,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    FSInputFile
)

# NOTE: psycopg (libpq) импортируем лениво внутри db(), чтобы приложение не падало,
# если в окружении нет libpq. Это устраняет ошибку "ImportError: no pq wrapper available".
_psycopg_loaded = False
_psycopg_error: Optional[Exception] = None


def _load_psycopg() -> bool:
    """Ленивая загрузка psycopg и зависимостей. Возвращает True/False."""
    global _psycopg_loaded, _psycopg_error
    if _psycopg_loaded:
        return True
    try:
        import psycopg  # type: ignore
        from psycopg.rows import dict_row  # type: ignore
        from psycopg.conninfo import conninfo_to_dict  # type: ignore
        globals()["psycopg"] = psycopg
        globals()["dict_row"] = dict_row
        globals()["conninfo_to_dict"] = conninfo_to_dict
        _psycopg_loaded = True
        return True
    except Exception as e:  # libpq не найден или другой импортный баг
        _psycopg_error = e
        _psycopg_loaded = False
        return False


# =============== utils ===============
def now_ts() -> datetime:
    return datetime.now(timezone.utc)


def _hash_hex(s: str) -> str:
    """MD5/SHA256 в HEX нижним регистром в зависимости от ROBOKASSA_SIGNATURE_ALG."""
    if ROBOKASSA_SIGNATURE_ALG == "SHA256":
        return hashlib.sha256(s.encode("utf-8")).hexdigest()
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _calc_extended_valid_until(old_vu: Optional[datetime], now_: datetime, days: int) -> datetime:
    """Продление от максимума(now, old_vu) на days."""
    base = old_vu if (old_vu and old_vu > now_) else now_
    return base + timedelta(days=days)


# =============== logging ===============
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")


# =============== env ===============
load_dotenv()

def _clean(v: Optional[str]) -> str:
    # убираем пробелы и случайные кавычки вокруг
    return (v or "").strip().strip('"').strip("'")

# Бот / вебхук / сайт
BOT_TOKEN = _clean(os.getenv("BOT_TOKEN"))
BASE_URL = _clean(os.getenv("BASE_URL")).rstrip("/")
WEBHOOK_SECRET = _clean(os.getenv("WEBHOOK_SECRET") or "secret")

# Канал / админ
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0") or 0)
ADMIN_USER_ID = (int(os.getenv("ADMIN_USER_ID", "0") or 0) or None)

# Robokassa
ROBOKASSA_LOGIN = _clean(os.getenv("ROBOKASSA_LOGIN"))
ROBOKASSA_PASSWORD1 = _clean(os.getenv("ROBOKASSA_PASSWORD1"))
ROBOKASSA_PASSWORD2 = _clean(os.getenv("ROBOKASSA_PASSWORD2"))
ROBOKASSA_SIGNATURE_ALG = (_clean(os.getenv("ROBOKASSA_SIGNATURE_ALG")) or "SHA256").upper()  # MD5|SHA256
if ROBOKASSA_SIGNATURE_ALG not in {"MD5", "SHA256"}:
    logger.error("ROBOKASSA_SIGNATURE_ALG must be 'MD5' or 'SHA256', got %s", ROBOKASSA_SIGNATURE_ALG)
    raise RuntimeError("Invalid ROBOKASSA_SIGNATURE_ALG")

ROBOKASSA_TEST_MODE = _clean(os.getenv("ROBOKASSA_TEST_MODE") or "0")  # "1" тест, "0" боевой

# Цена — строго 2 знака
PRICE_RUB = Decimal(_clean(os.getenv("PRICE_RUB") or "10.00")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
SUBSCRIPTION_DAYS = int(os.getenv("SUBSCRIPTION_DAYS", "30"))
REMIND_DAYS_BEFORE = int(os.getenv("REMIND_DAYS_BEFORE", "3"))

def money2(x) -> str:
    # безопасно приводим к Decimal и фиксируем ДВЕ цифры
    d = Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return format(d, ".2f")

# БД: можно одной строкой или по полям (для Supabase pooler 6543)
DATABASE_URL = _clean(os.getenv("DATABASE_URL"))
DB_HOST = _clean(os.getenv("DB_HOST"))
DB_PORT = int(os.getenv("DB_PORT", "6543"))
DB_NAME = _clean(os.getenv("DB_NAME") or "postgres")
DB_USER = _clean(os.getenv("DB_USER"))
DB_PASSWORD = _clean(os.getenv("DB_PASSWORD"))
PROJECT_REF = _clean(os.getenv("PROJECT_REF"))  # напр., ajcommzzdmzpyzzqclgb

# Режим БД при отсутствии драйвера: 'strict' (ошибка) или 'mock' (заглушка)
DB_FALLBACK_MODE = _clean(os.getenv("DB_FALLBACK_MODE") or "strict").lower()


# =============== FastAPI & static ===============
app = FastAPI(title="TG Sub Bot")
os.makedirs("static", exist_ok=True)
os.makedirs("assets", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def root():
    return HTMLResponse("<h3>OK: бот работает. Документы — по кнопкам в боте.</h3>")


@app.get("/health")
def health():
    status = {"status": "ok"}
    if not _load_psycopg():
        status["db_driver"] = "missing"
    return status


# =============== Aiogram (создаём ДО декораторов) ===============
if not BOT_TOKEN or not BASE_URL:
    logger.warning("⚠️ BOT_TOKEN и/или BASE_URL не заданы — проверь .env")
bot = Bot(BOT_TOKEN) if BOT_TOKEN else None
dp = Dispatcher()
loop_task: Optional[asyncio.Task] = None


@dp.errors()
async def on_aiogram_error(event):
    logger.exception("Aiogram handler error", exc_info=event.exception if hasattr(event, "exception") else None)
    if ADMIN_USER_ID and bot:
        try:
            await bot.send_message(
                ADMIN_USER_ID,
                f"⚠️ Ошибка: {getattr(event, 'exception', None)}"
            )
        except Exception:
            pass
    return True


WELCOME_IMAGE_PATH = "assets/welcome.png"
EMAIL_RE = re.compile(r"^[A-Za-z0-9_.+\-]+@[A-Za-z0-9\-]+\.[A-Za-z0-9\.\-]+$")

main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="💳 Оплатить подписку"), KeyboardButton(text="🔁 Продлить подписку")],
        [KeyboardButton(text="📄 Документы")],
        [KeyboardButton(text="📊 Мой статус")],
    ],
    resize_keyboard=True
)


# ============================== DB helpers ==============================
# В этом модуле мы не импортируем psycopg на верхнем уровне — только здесь, при вызове.
async def db() -> Any:
    """
    Подключение к БД.
    - Пытаемся лениво загрузить psycopg. Если не получается и DB_FALLBACK_MODE='mock',
      возвращаем заглушку, чтобы приложение могло работать без БД (ограниченно).
    - Если DB_FALLBACK_MODE='strict' (по умолчанию) — бросаем понятную ошибку.
    """
    if not _load_psycopg():
        msg = (
            "PostgreSQL driver (psycopg/libpq) недоступен. Установите пакет 'psycopg[binary]' "
            "или libpq в окружение. Техническая причина: %r" % _psycopg_error
        )
        if DB_FALLBACK_MODE == "mock":
            return _MockConn()
        raise RuntimeError(msg)

    # psycopg загружен — собираем DSN и подключаемся
    psycopg = globals()["psycopg"]
    dict_row = globals()["dict_row"]
    conninfo_to_dict = globals()["conninfo_to_dict"]

    safe_params: dict = {}
    try:
        if DATABASE_URL:
            conn_str = DATABASE_URL
            # Автодобавление options=project=... для Supabase pooler (порт 6543)
            if PROJECT_REF:
                try:
                    # Разбираем и при необходимости добавляем options=project=...
                    parsed = urlparse(conn_str)
                    qs_pairs = parse_qsl(parsed.query, keep_blank_values=True)
                    port = parsed.port
                    has_project = any(
                        k == "options" and "project=" in v for k, v in qs_pairs
                    )
                    if (
                        PROJECT_REF
                        and port == 6543
                        and not has_project
                    ):
                        qs_pairs.append(("options", f"project={PROJECT_REF}"))
                        new_query = urlencode(qs_pairs, doseq=True)
                        conn_str = urlunparse(parsed._replace(query=new_query))

                    d = conninfo_to_dict(conn_str)
                    d.pop("password", None)
                    safe_params = d
                except Exception:
                    safe_params = {"dsn": "DATABASE_URL"}
        else:
            host = DB_HOST or "aws-1-eu-north-1.pooler.supabase.com"
            port = int(DB_PORT or 6543)
            name = DB_NAME or "postgres"
            user = DB_USER
            pwd = DB_PASSWORD

            if not user or not pwd:
                raise RuntimeError("DB_USER/DB_PASSWORD не заданы в переменных окружения.")

            # Для Supabase pooler обязателен sslmode=require, а также options=project=PROJECT_REF
            if port == 6543 and not PROJECT_REF:
                raise RuntimeError(
                    "PROJECT_REF не задан для Supabase pooler (порт 6543). "
                    "Задайте PROJECT_REF (идентификатор проекта из Supabase) или используйте DB_PORT=5432/DATABASE_URL из панели."
                )
            options_part = f" options=project={PROJECT_REF}" if PROJECT_REF else ""
            conn_str = (
                f"host={host} port={port} dbname={name} user={user} password={pwd} sslmode=require" + options_part
            )
            safe_params = {"host": host, "port": port, "dbname": name, "user": user}
            if PROJECT_REF:
                safe_params["options"] = f"project={PROJECT_REF}"

        return await psycopg.AsyncConnection.connect(conn_str, row_factory=dict_row, connect_timeout=10)
    except psycopg.OperationalError as e:
        logger.exception("DB connection failed. params=%s", safe_params)
        raise RuntimeError(f"Не удалось подключиться к базе данных: {e}") from e
    except Exception as e:
        logger.exception("DB connection failed. params=%s", safe_params)
        raise RuntimeError("Не удалось подключиться к базе данных.") from e


# ------------------------------ MOCK LAYER ------------------------------
# Минимальная заглушка БД для локального прогона без libpq: хранение в памяти.
# Только то, что реально вызывается кодом (INSERT/UPDATE/SELECT нужных полей).
class _MockCursor:
    def __init__(self, dbref: "_MockConn"):
        self._db = dbref
        self._rows: list[dict] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, sql: str, params: tuple | list | None = None):
        sql = sql.strip().lower()
        p = list(params or [])
        # users table
        if sql.startswith("select * from users where tg_id="):
            tg_id = p[0]
            row = self._db.users.get(tg_id)
            self._rows = [row] if row else []
        elif sql.startswith("insert into users"):
            cols_part = sql.split("(",1)[1].split(")",1)[0].replace(" ","")
            cols = cols_part.split(",")
            rec = dict(zip(cols, p))
            tg_id = rec.get("tg_id")
            self._db.users[tg_id] = rec
            self._rows = []
        elif sql.startswith("update users set") and sql.endswith("where tg_id=%s"):
            tg_id = p[-1]
            user = self._db.users.get(tg_id, {"tg_id": tg_id})
            set_clause = sql.split("set",1)[1].rsplit("where",1)[0]
            keys = [seg.split("=")[0].strip() for seg in set_clause.split(",")]
            for k, v in zip(keys, p[:-1]):
                user[k] = v
            self._db.users[tg_id] = user
            self._rows = []
        elif sql.startswith("select tg_id, valid_until, remind_3d_sent from users"):
            rows = []
            for u in self._db.users.values():
                if u.get("status") == "active" and u.get("valid_until") is not None:
                    rows.append({
                        "tg_id": u.get("tg_id"),
                        "valid_until": u.get("valid_until"),
                        "remind_3d_sent": u.get("remind_3d_sent", 0)
                    })
            self._rows = rows
        # payments table
        elif sql.startswith("insert into payments"):
            inv_id = self._db.next_inv_id
            self._db.next_inv_id += 1
            tg_id, out_sum, status, created_at = p
            self._db.payments[inv_id] = {
                "inv_id": inv_id, "tg_id": tg_id, "out_sum": out_sum,
                "status": status, "created_at": created_at, "paid_at": None, "signature": None
            }
            self._rows = [{"inv_id": inv_id}]
        elif sql.startswith("update payments set status='paid'"):
            paid_at, inv_id = p
            if inv_id in self._db.payments:
                self._db.payments[inv_id]["status"] = "paid"
                self._db.payments[inv_id]["paid_at"] = paid_at
            self._rows = []
        elif sql.startswith("update payments set status='failed'"):
            inv_id = p[0]
            if inv_id in self._db.payments:
                self._db.payments[inv_id]["status"] = "failed"
            self._rows = []
        elif sql.startswith("select tg_id from payments where inv_id="):
            inv_id = p[0]
            row = self._db.payments.get(inv_id)
            self._rows = [{"tg_id": row["tg_id"]}] if row else []
        # legal_confirms & doc_views
        elif sql.startswith("insert into legal_confirms"):
            self._rows = []
        elif sql.startswith("insert into doc_views"):
            self._rows = []
        elif sql.startswith("update users set policy_viewed_at=") or \
             sql.startswith("update users set consent_viewed_at=") or \
             sql.startswith("update users set offer_viewed_at=") or \
             sql.startswith("update users set legal_confirmed_at="):
            token = p[-1] if "policy_token=%s" in sql else None
            if token is not None:
                for u in self._db.users.values():
                    if u.get("policy_token") == token:
                        if sql.startswith("update users set policy_viewed_at="):
                            u["policy_viewed_at"] = p[0]
                        elif sql.startswith("update users set consent_viewed_at="):
                            u["consent_viewed_at"] = p[0]
                        elif sql.startswith("update users set offer_viewed_at="):
                            u["offer_viewed_at"] = p[0]
            self._rows = []
        elif sql.startswith("select tg_id from users where policy_token="):
            token = p[0]
            found = None
            for u in self._db.users.values():
                if u.get("policy_token") == token:
                    found = {"tg_id": u.get("tg_id")}
                    break
            self._rows = [found] if found else []
        else:
            self._rows = []

    async def fetchone(self):
        return self._rows[0] if self._rows else None

    async def fetchall(self):
        return list(self._rows)


class _MockConn:
    def __init__(self):
        self.users: dict[int, dict] = {}
        self.payments: dict[int, dict] = {}
        self.next_inv_id = 1

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _MockCursor(self)

    async def commit(self):
        return


# =========================== High-level DB API ===========================
async def init_db():
    """Создаёт/мигрирует таблицы и индексы (идемпотентно).
    В mock-режиме — no-op.
    """
    try:
        async with await db() as con:
            async with con.cursor() as cur:
                await cur.execute(
                    dedent("""
                        CREATE TABLE IF NOT EXISTS users (
                            tg_id BIGINT PRIMARY KEY,
                            created_at TIMESTAMPTZ,
                            updated_at TIMESTAMPTZ,
                            email TEXT,
                            phone TEXT,
                            status TEXT DEFAULT 'new',
                            policy_token TEXT,
                            policy_viewed_at TIMESTAMPTZ,
                            consent_viewed_at TIMESTAMPTZ,
                            offer_viewed_at TIMESTAMPTZ,
                            legal_confirmed_at TIMESTAMPTZ,
                            valid_until TIMESTAMPTZ,
                            last_invoice_id BIGINT,
                            remind_3d_sent INT DEFAULT 0,
                            last_pay_msg_id BIGINT
                        );
                    """)
                )
                await cur.execute("CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);")

                await cur.execute(
                    dedent("""
                        CREATE TABLE IF NOT EXISTS payments (
                            inv_id BIGSERIAL PRIMARY KEY,
                            tg_id BIGINT,
                            out_sum NUMERIC(12,2),
                            status TEXT,
                            created_at TIMESTAMPTZ,
                            paid_at TIMESTAMPTZ,
                            signature TEXT
                        );
                    """)
                )
                await cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_tg ON payments(tg_id);")

                await cur.execute(
                    dedent("""
                        CREATE TABLE IF NOT EXISTS legal_confirms (
                            id BIGSERIAL PRIMARY KEY,
                            tg_id BIGINT,
                            token TEXT,
                            confirmed_at TIMESTAMPTZ DEFAULT now()
                        );
                    """)
                )
                await cur.execute("CREATE INDEX IF NOT EXISTS idx_legal_confirms_tg ON legal_confirms(tg_id);")

                await cur.execute(
                    dedent("""
                        CREATE TABLE IF NOT EXISTS doc_views (
                            id BIGSERIAL PRIMARY KEY,
                            tg_id BIGINT,
                            token TEXT,
                            doc_type TEXT,
                            ip TEXT,
                            user_agent TEXT,
                            opened_at TIMESTAMPTZ DEFAULT now()
                        );
                    """)
                )
                await cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_views_token ON doc_views(token);")
                await cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_views_tg ON doc_views(tg_id);")

                await con.commit()
    except Exception as e:
        logger.error("init_db failed: %s", e)


async def get_user(tg_id: int):
    try:
        async with await db() as con:
            async with con.cursor() as cur:
                await cur.execute("SELECT * FROM users WHERE tg_id=%s", (tg_id,))
                return await cur.fetchone()
    except Exception as e:
        logger.error("get_user failed: %s", e)
        return None


async def upsert_user(tg_id: int, **kwargs):
    try:
        reserved = {"tg_id", "created_at", "updated_at"}
        data = {k: v for k, v in kwargs.items() if k not in reserved}
        row = await get_user(tg_id)
        if row is None:
            cols = ["tg_id", "created_at", "updated_at"] + list(data.keys())
            vals = [tg_id, now_ts(), now_ts()] + list(data.values())
            ph = ["%s"] * len(cols)
            async with await db() as con:
                async with con.cursor() as cur:
                    await cur.execute(
                        f"INSERT INTO users({','.join(cols)}) VALUES({','.join(ph)})",
                        tuple(vals),
                    )
                    await con.commit()
        else:
            if data:
                sets = [f"{k}=%s" for k in data] + ["updated_at=%s"]
                vals = list(data.values()) + [now_ts(), tg_id]
                async with await db() as con:
                    async with con.cursor() as cur:
                        await cur.execute(
                            f"UPDATE users SET {', '.join(sets)} WHERE tg_id=%s",
                            tuple(vals),
                        )
                        await con.commit()
            else:
                async with await db() as con:
                    async with con.cursor() as cur:
                        await cur.execute(
                            "UPDATE users SET updated_at=%s WHERE tg_id=%s",
                            (now_ts(), tg_id),
                        )
                        await con.commit()
    except Exception as e:
        logger.error("upsert_user failed: %s", e)


async def list_active_users():
    try:
        async with await db() as con:
            async with con.cursor() as cur:
                await cur.execute(
                    "SELECT tg_id, valid_until, remind_3d_sent FROM users WHERE status='active' AND valid_until IS NOT NULL"
                )
                return await cur.fetchall()
    except Exception as e:
        logger.error("list_active_users failed: %s", e)
        return []


# ============================ Robokassa logic ============================
def sign_form(out_sum, inv_id: int) -> str:
    # Подпись для платёжной ссылки (кабинет может требовать MD5 или SHA256)
    base = f"{ROBOKASSA_LOGIN}:{money2(out_sum)}:{inv_id}:{ROBOKASSA_PASSWORD1}"
    logger.info("RK base(form) %s", base.replace(ROBOKASSA_PASSWORD1, "***"))
    return _hash_hex(base)


def sign_result_from_raw(out_sum_str: str, inv_id: int) -> str:
    # OutSum берём как пришёл от Робокассы (строкой), без форматирования
    base = f"{out_sum_str}:{inv_id}:{ROBOKASSA_PASSWORD2}"
    logger.info("RK base(result) %s", base.replace(ROBOKASSA_PASSWORD2, "***"))
    return _hash_hex(base)


def sign_success_from_raw(out_sum_str: str, inv_id: int) -> str:
    # Success-redirect подпись: OutSum:InvId:Password1 (без MerchantLogin)
    base = f"{out_sum_str}:{inv_id}:{ROBOKASSA_PASSWORD1}"
    logger.info("RK base(success_cb) %s", base.replace(ROBOKASSA_PASSWORD1, "***"))
    return _hash_hex(base)


def build_pay_url(inv_id: int, out_sum, description: str) -> str:
    if not ROBOKASSA_LOGIN or not ROBOKASSA_PASSWORD1:
        missing = []
        if not ROBOKASSA_LOGIN:
            missing.append("ROBOKASSA_LOGIN")
        if not ROBOKASSA_PASSWORD1:
            missing.append("ROBOKASSA_PASSWORD1")
        raise RuntimeError(f"Robokassa credentials missing: {', '.join(missing)}")

    sig = sign_form(out_sum, inv_id)
    params = {
        "MerchantLogin": ROBOKASSA_LOGIN,
        "OutSum":        money2(out_sum),
        "InvId":         str(inv_id),
        "Description":   description,
        "SignatureValue": sig,
        "Culture":       "ru",
        "Encoding":      "utf-8",
        "IsTest":        "0" if ROBOKASSA_TEST_MODE == "0" else "1",
        "SuccessURL":    f"{BASE_URL}/pay/success",
        "FailURL":       f"{BASE_URL}/pay/fail",
    }
    url = "https://auth.robokassa.ru/Merchant/Index.aspx?" + urlencode(params)
    logger.info("RK LINK -> InvId=%s OutSum=%s sig=%s", inv_id, money2(out_sum), sig)
    return url


async def new_payment(tg_id: int, out_sum) -> int:
    async with await db() as con:
        async with con.cursor() as cur:
            await cur.execute(
                "INSERT INTO payments(tg_id, out_sum, status, created_at) VALUES(%s,%s,%s,%s) RETURNING inv_id",
                (tg_id, Decimal(str(out_sum)), "created", now_ts()),
            )
            row = await cur.fetchone()
            inv_id = (row or {}).get("inv_id")
            if not inv_id:
                inv_id = getattr(con, "next_inv_id", 1)
            await con.commit()
    await upsert_user(tg_id, last_invoice_id=inv_id)
    return int(inv_id)


async def set_payment_paid(inv_id: int):
    async with await db() as con:
        async with con.cursor() as cur:
            await cur.execute(
                "UPDATE payments SET status='paid', paid_at=%s WHERE inv_id=%s",
                (now_ts(), inv_id),
            )
            await con.commit()


def pay_kb(url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💳 Оплатить {money2(PRICE_RUB)} ₽ через Robokassa", url=url)]
    ])


# =============================== Документы ===============================
def legal_keyboard(token: str) -> InlineKeyboardMarkup:
    """Только одна кнопка — подтверждение."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✔️ Подтвердить ознакомление", callback_data=f"legal_agree:{token}")]
    ])


def docs_keyboard(token: str) -> InlineKeyboardMarkup:
    """Клавиатура со ссылками на документы (по желанию пользователя)."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Политика конфиденциальности", url=f"{BASE_URL}/policy/{token}")],
        [InlineKeyboardButton(text="✅ Согласие на обработку данных", url=f"{BASE_URL}/consent/{token}")],
        [InlineKeyboardButton(text="📑 Публичная оферта", url=f"{BASE_URL}/offer/{token}")],
    ])


async def get_or_make_token(tg_id: int) -> str:
    u = await get_user(tg_id)
    if u and u.get("policy_token"):
        return u["policy_token"]
    token = secrets.token_urlsafe(16)
    await upsert_user(tg_id, policy_token=token, status="new")
    return token


async def _legal_ok(tg_id: int) -> bool:
    try:
        async with await db() as con:
            async with con.cursor() as cur:
                await cur.execute("SELECT legal_confirmed_at FROM users WHERE tg_id=%s", (tg_id,))
                r = await cur.fetchone()
        return bool(r and r.get("legal_confirmed_at"))
    except Exception as e:
        logger.error("_legal_ok failed: %s", e)
        return False


# ============================== Bot handlers =============================
@dp.message(CommandStart())
async def on_start(message: Message):
    token = await get_or_make_token(message.from_user.id)
    txt = (
        "✨ Добро пожаловать в канал «Погружаясь в Кундалини»!\n"
        "Здесь мы работаем с дыханием, мантрами и медитативным движением.\n\n"
        "Нажмите «✔️ Подтвердить ознакомление», чтобы продолжить.\n"
        "Документы можно открыть в меню «📄 Документы»."
    )
    try:
        await message.answer_photo(FSInputFile(WELCOME_IMAGE_PATH), caption=txt, reply_markup=legal_keyboard(token))
    except Exception:
        await message.answer(txt, reply_markup=legal_keyboard(token))
    await message.answer("Меню ниже 👇", reply_markup=main_menu)


@dp.message(Command("help"))
async def on_help(message: Message):
    await message.answer(
        "Команды:\n"
        "/start — начать\n"
        "/pay — оплата (после согласия)\n"
        "/renew — продлить подписку\n"
        "/stats — статус подписки\n"
        "/help — помощь"
    )


@dp.message(F.text == "📄 Документы")
@dp.message(Command("docs"))
async def on_docs(message: Message):
    token = await get_or_make_token(message.from_user.id)
    await message.answer("Документы:", reply_markup=docs_keyboard(token))


@dp.callback_query(F.data.startswith("legal_agree:"))
async def on_legal_agree(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]

    # ищем пользователя по токену
    async with await db() as con:
        async with con.cursor() as cur:
            await cur.execute("SELECT tg_id FROM users WHERE policy_token=%s", (token,))
            row = await cur.fetchone()

    if not row:
        await cb.answer("Сессия не найдена. Нажмите /start", show_alert=True)
        return

    tg_id = row["tg_id"]

    # фиксируем согласие + аудит
    async with await db() as con:
        async with con.cursor() as cur:
            await cur.execute(
                "UPDATE users SET legal_confirmed_at=%s, status=%s WHERE tg_id=%s",
                (now_ts(), "legal_ok", tg_id),
            )
            await cur.execute(
                "INSERT INTO legal_confirms(tg_id, token, confirmed_at) VALUES (%s,%s,%s)",
                (tg_id, token, now_ts()),
            )
            await con.commit()

    # создаём один (!) платёж и сразу строим ссылку
    inv_id = await new_payment(tg_id, PRICE_RUB)
    url = build_pay_url(inv_id, PRICE_RUB, "Подписка на 30 дней")

    await cb.message.answer("Спасибо! ✅ Теперь можно оплатить:", reply_markup=pay_kb(url))
    await cb.answer()


@dp.message(F.text == "💳 Оплатить подписку")
@dp.message(Command("pay"))
async def on_pay(message: Message):
    tg_id = message.from_user.id
    if not await _legal_ok(tg_id):
        token = await get_or_make_token(tg_id)
        await message.answer(
            "Сначала подтвердите ознакомление с документами:",
            reply_markup=legal_keyboard(token)
        )
        return

    try:
        inv_id = await new_payment(tg_id, PRICE_RUB)
        url = build_pay_url(inv_id, PRICE_RUB, "Подписка на 30 дней")
        await message.answer("Готово! Нажмите, чтобы оплатить:", reply_markup=pay_kb(url))
    except Exception as e:
        logger.error("/pay failed: %s", e)
        await message.answer("⚠️ Временно недоступно. Попробуйте позже.")


@dp.message(F.text == "🔁 Продлить подписку")
@dp.message(Command("renew"))
async def on_renew(message: Message):
    tg_id = message.from_user.id
    if not await _legal_ok(tg_id):
        token = await get_or_make_token(tg_id)
        await message.answer(
            "Сначала подтвердите ознакомление с документами:",
            reply_markup=legal_keyboard(token)
        )
        return
    try:
        inv_id = await new_payment(tg_id, PRICE_RUB)
        url = build_pay_url(inv_id, PRICE_RUB, f"Продление на {SUBSCRIPTION_DAYS} дней")
        await message.answer("Продлить подписку:", reply_markup=pay_kb(url))
    except Exception as e:
        logger.error("/renew failed: %s", e)
        await message.answer("⚠️ Временно недоступно. Попробуйте позже.")


def bar(progress: float, width: int = 20) -> str:
    filled = int(round(progress * width))
    return "▮" * filled + "▯" * (width - filled)


@dp.message(F.text == "📊 Мой статус")
@dp.message(Command("stats"))
async def on_stats(message: Message):
    u = await get_user(message.from_user.id)
    if not u or not u.get("valid_until"):
        await message.answer("Подписка пока не активна.")
        return

    vu = u["valid_until"]
    if isinstance(vu, str):
        try:
            vu = datetime.fromisoformat(vu)
        except Exception:
            await message.answer("Не удалось разобрать дату окончания.")
            return

    now = datetime.now(timezone.utc)
    total = timedelta(days=SUBSCRIPTION_DAYS)
    left = max(vu - now, timedelta(0))
    used = total - left
    progress = float(min(max(used / total, 0), 1))
    days_left = int(left.total_seconds() // 86400)
    hours_left = int((left.total_seconds() % 86400) // 3600)
    text = (
        f"📊 Статус подписки\n"
        f"До окончания: {days_left} д. {hours_left} ч.\n"
        f"`{bar(progress)}` {int(progress*100)}%\n"
        f"Действует до: {vu.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    await message.answer(text, parse_mode="Markdown")


@dp.message(F.text & ~F.text.regexp(r"^/"))
async def on_text(message: Message):
    await message.answer("Напишите /help для списка команд.")


# ================= Документные страницы (фиксируют просмотр) =================
def _read_html(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


@app.get("/policy/{token}", response_class=HTMLResponse)
async def policy_with_token(token: str, request: Request):
    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent", "")
    try:
        async with await db() as con:
            async with con.cursor() as cur:
                await cur.execute(
                    "UPDATE users SET policy_viewed_at=%s WHERE policy_token=%s",
                    (now_ts(), token),
                )
                await cur.execute(
                    """
                    INSERT INTO doc_views(tg_id, token, doc_type, ip, user_agent)
                    SELECT tg_id, %s, %s, %s, %s FROM users WHERE policy_token=%s
                    """,
                    (token, "policy", ip, ua, token),
                )
                await con.commit()
    except Exception as e:
        logger.error("policy update failed: %s", e)
    logger.info("DOC VIEW: type=policy token=%s ip=%s", token, ip)
    return HTMLResponse(_read_html("static/policy.html"))


@app.get("/consent/{token}", response_class=HTMLResponse)
async def consent_with_token(token: str, request: Request):
    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent", "")
    try:
        async with await db() as con:
            async with con.cursor() as cur:
                await cur.execute(
                    "UPDATE users SET consent_viewed_at=%s WHERE policy_token=%s",
                    (now_ts(), token),
                )
                await cur.execute(
                    """
                    INSERT INTO doc_views(tg_id, token, doc_type, ip, user_agent)
                    SELECT tg_id, %s, %s, %s, %s FROM users WHERE policy_token=%s
                    """,
                    (token, "consent", ip, ua, token),
                )
                await con.commit()
    except Exception as e:
        logger.error("consent update failed: %s", e)
    logger.info("DOC VIEW: type=consent token=%s ip=%s", token, ip)
    return HTMLResponse(_read_html("static/consent.html"))


@app.get("/offer/{token}", response_class=HTMLResponse)
async def offer_with_token(token: str, request: Request):
    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent", "")
    try:
        async with await db() as con:
            async with con.cursor() as cur:
                await cur.execute(
                    "UPDATE users SET offer_viewed_at=%s WHERE policy_token=%s",
                    (now_ts(), token),
                )
                await cur.execute(
                    """
                    INSERT INTO doc_views(tg_id, token, doc_type, ip, user_agent)
                    SELECT tg_id, %s, %s, %s, %s FROM users WHERE policy_token=%s
                    """,
                    (token, "offer", ip, ua, token),
                )
                await con.commit()
    except Exception as e:
        logger.error("offer update failed: %s", e)
    logger.info("DOC VIEW: type=offer token=%s ip=%s", token, ip)
    return HTMLResponse(_read_html("static/offer.html"))


# Plain-страницы для ручной проверки (без фиксации)
@app.get("/policy", response_class=HTMLResponse)
def policy_plain():
    return HTMLResponse(_read_html("static/policy.html"))


@app.get("/consent", response_class=HTMLResponse)
def consent_plain():
    return HTMLResponse(_read_html("static/consent.html"))


@app.get("/offer", response_class=HTMLResponse)
def offer_plain():
    return HTMLResponse(_read_html("static/offer.html"))


# ======================== Robokassa callbacks ========================
class RobokassaResult(BaseModel):
    OutSum: float
    InvId: int
    SignatureValue: str


def _eq_ci(a: str, b: str) -> bool:
    return (a or "").lower() == (b or "").lower()


@app.post("/pay/result")
async def pay_result(request: Request):
    data = dict(await request.form())
    try:
        out_sum_raw = data.get("OutSum")  # строка как есть!
        inv_id = int(data.get("InvId"))
        sig = data.get("SignatureValue") or ""
    except Exception:
        raise HTTPException(400, "Bad payload")

    expected = sign_result_from_raw(out_sum_raw, inv_id)
    if not _eq_ci(sig, expected):
        try:
