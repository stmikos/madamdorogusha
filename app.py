# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import asyncio
import logging
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from hashlib import md5, sha256
from textwrap import dedent
from urllib.parse import urlencode, quote_plus  # noqa: F401

import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    CallbackQuery,
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    FSInputFile,
    ErrorEvent,
)

# ======================= utils & logging =======================
def now_ts() -> datetime:
    return datetime.now(timezone.utc)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

load_dotenv()

# ======================= env =======================
def _clean(v: str | None) -> str:
    return (v or "").strip().strip('"').strip("'")

# Бот / вебхук / сайт
BOT_TOKEN = _clean(os.getenv("BOT_TOKEN"))
BASE_URL = _clean(os.getenv("BASE_URL")).rstrip("/")
WEBHOOK_SECRET = _clean(os.getenv("WEBHOOK_SECRET") or "Madamgorogusha")

# Канал / админ
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0") or 0)
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0") or 0) or None

# Robokassa
ROBOKASSA_LOGIN = _clean(os.getenv("ROBOKASSA_LOGIN"))
ROBOKASSA_PASSWORD1 = _clean(os.getenv("ROBOKASSA_PASSWORD1"))
ROBOKASSA_PASSWORD2 = _clean(os.getenv("ROBOKASSA_PASSWORD2"))
ROBOKASSA_SIGNATURE_ALG = (_clean(os.getenv("ROBOKASSA_SIGNATURE_ALG")) or "SHA256").upper()  # MD5|SHA256
ROBOKASSA_TEST_MODE = _clean(os.getenv("ROBOKASSA_TEST_MODE") or "0")  # "1" тест, "0" боевой

# Цена и срок
try:
    PRICE_RUB = float(_clean(os.getenv("PRICE_RUB") or "10"))
except Exception:
    PRICE_RUB = 10.0
SUBSCRIPTION_DAYS = int(os.getenv("SUBSCRIPTION_DAYS", "30"))

# БД (Supabase Pooler)
DB_HOST = _clean(os.getenv("DB_HOST") or "aws-1-eu-central-1.pooler.supabase.com")
DB_PORT = int(os.getenv("DB_PORT", "6543"))
DB_NAME = _clean(os.getenv("DB_NAME") or "postgres")
DB_USER = _clean(os.getenv("DB_USER") or "postgres")  # ожидается 'postgres.<project_ref>'
DB_PASSWORD = _clean(os.getenv("DB_PASSWORD"))
PROJECT_REF = _clean(os.getenv("PROJECT_REF"))  # опционально; если задан и DB_USER без суффикса — добавим
DATABASE_URL = _clean(os.getenv("DATABASE_URL"))  # игнорируем, чтобы не было options=...

# ======================= FastAPI & static =======================
app = FastAPI(title="TG Sub Bot")
os.makedirs("static", exist_ok=True)
os.makedirs("assets", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def root():
    return HTMLResponse("<h3>OK: бот работает. Документы — по кнопкам в боте.</h3>")

@app.get("/health")
def health():
    return {"status": "ok"}

# ======================= Aiogram =======================
if not BOT_TOKEN or not BASE_URL:
    logger.warning("⚠️ BOT_TOKEN и/или BASE_URL не заданы — проверь env")

bot = Bot(BOT_TOKEN) if BOT_TOKEN else None
dp = Dispatcher()
loop_task: asyncio.Task | None = None

@dp.errors()
async def on_aiogram_error(event: ErrorEvent):
    logger.exception("Aiogram handler error", exc_info=event.exception)
    if ADMIN_USER_ID and bot:
        try:
            await bot.send_message(
                ADMIN_USER_ID,
                f"⚠️ Ошибка: {type(event.exception).__name__}\n{event.exception}"
            )
        except Exception:
            pass
    return True

WELCOME_IMAGE_PATH = "assets/welcome.png"
EMAIL_RE = re.compile(r"^[A-Za-z0-9_.+\-]+@[A-Za-z0-9\-]+\.[A-Za-z0-9\.\-]+$")

main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="💳 Оплатить подписку")],
        [KeyboardButton(text="📄 Документы")],
        [KeyboardButton(text="📊 Мой статус")],
    ],
    resize_keyboard=True,
)

# ======================= DB connection =======================
def _compose_kw_from_env():
    user = DB_USER or "postgres"
    if "." not in user and PROJECT_REF:
        user = f"{user}.{PROJECT_REF}"

    kw = dict(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=user,
        password=DB_PASSWORD,
        sslmode="require",
        row_factory=dict_row,
        connect_timeout=10,
    )
    return kw

@asynccontextmanager
async def db():
    """Подключение к БД через kwargs (без DATABASE_URL и без options)."""
    if not DB_USER or not DB_PASSWORD:
        raise RuntimeError("DB_USER/DB_PASSWORD must be set")

    kw = _compose_kw_from_env()
    logger.info("[DB CFG] host=%s port=%s db=%s user=%s sslmode=require (no options, no DATABASE_URL)",
                kw["host"], kw["port"], kw["dbname"], kw["user"])
    conn = await psycopg.AsyncConnection.connect(**kw)
    try:
        yield conn
    finally:
        await conn.close()

# ======================= DB helpers =======================
async def init_db():
    try:
        async with db() as con:
            async with con.cursor() as cur:
                # users
                await cur.execute(dedent("""
                    CREATE TABLE IF NOT EXISTS users (
                        tg_id BIGINT PRIMARY KEY,
                        created_at TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ,
                        email TEXT,
                        phone TEXT,
                        status TEXT DEFAULT 'new',
                        policy_token TEXT,
                        legal_confirmed_at TIMESTAMPTZ,
                        valid_until TIMESTAMPTZ,
                        last_invoice_id BIGINT,
                        remind_3d_sent INT DEFAULT 0,
                        last_pay_msg_id BIGINT
                    );
                """))
                await cur.execute("CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);")

                # payments
                await cur.execute(dedent("""
                    CREATE TABLE IF NOT EXISTS payments (
                        inv_id BIGSERIAL PRIMARY KEY,
                        tg_id BIGINT,
                        out_sum NUMERIC(12,2),
                        status TEXT,
                        created_at TIMESTAMPTZ,
                        paid_at TIMESTAMPTZ,
                        signature TEXT
                    );
                """))
                await cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_tg ON payments(tg_id);")

                # лог подтверждения ознакомления
                await cur.execute(dedent("""
                    CREATE TABLE IF NOT EXISTS legal_confirms (
                        id BIGSERIAL PRIMARY KEY,
                        tg_id BIGINT,
                        token TEXT,
                        confirmed_at TIMESTAMPTZ DEFAULT now()
                    );
                """))
                await cur.execute("CREATE INDEX IF NOT EXISTS idx_legal_confirms_tg ON legal_confirms(tg_id);")

            await con.commit()
    except Exception as e:
        logger.error("init_db failed: %s", e)

async def get_user(tg_id: int):
    try:
        async with db() as con:
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
            async with db() as con:
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
                async with db() as con:
                    async with con.cursor() as cur:
                        await cur.execute(
                            f"UPDATE users SET {', '.join(sets)} WHERE tg_id=%s",
                            tuple(vals),
                        )
                    await con.commit()
            else:
                async with db() as con:
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
        async with db() as con:
            async with con.cursor() as cur:
                await cur.execute("""
                    SELECT tg_id, valid_until, remind_3d_sent
                    FROM users WHERE status='active' AND valid_until IS NOT NULL
                """)
                return await cur.fetchall()
    except Exception as e:
        logger.error("list_active_users failed: %s", e)
        return []

# ======================= Robokassa =======================
def money2(x) -> str:
    try:
        f = float(x)
    except Exception:
        f = 0.0
    return f"{f:.2f}"

def _hash_hex(s: str) -> str:
    if ROBOKASSA_SIGNATURE_ALG == "SHA256":
        return sha256(s.encode("utf-8")).hexdigest().upper()
    return md5(s.encode("utf-8")).hexdigest().upper()

def sign_success(out_sum, inv_id: int) -> str:
    # MerchantLogin:OutSum:InvId:Password1
    base = f"{ROBOKASSA_LOGIN}:{money2(out_sum)}:{inv_id}:{ROBOKASSA_PASSWORD1}"
    logger.info("RK base(success)=%s", base.replace(ROBOKASSA_PASSWORD1, "***"))
    return _hash_hex(base)

def sign_result_from_raw(out_sum_raw: str, inv_id: int) -> str:
    # OutSum (ровно как пришёл) : InvId : Password2
    base = f"{out_sum_raw}:{inv_id}:{ROBOKASSA_PASSWORD2}"
    logger.info("RK base(result)=%s", base.replace(ROBOKASSA_PASSWORD2, "***"))
    return _hash_hex(base)

def build_pay_url(inv_id: int, out_sum, description: str = "Подписка на 30 дней") -> str:
    if not ROBOKASSA_LOGIN or not ROBOKASSA_PASSWORD1:
        raise RuntimeError("Robokassa credentials missing")

    sig = sign_success(out_sum, inv_id)
    params = {
        "MerchantLogin": ROBOKASSA_LOGIN,
        "OutSum":        money2(out_sum),
        "InvId":         str(inv_id),
        "Description":   description,
        "SignatureValue": sig,
        "Culture":       "ru",
        "Encoding":      "utf-8",
        "IsTest":        "1" if ROBOKASSA_TEST_MODE == "1" else "0",
    }
    url = "https://auth.robokassa.ru/Merchant/Index.aspx?" + urlencode(params)
    safe_log = {k: v for k, v in params.items() if k != "SignatureValue"}
    logger.info("[RK DEBUG] %s", safe_log)
    return url

async def new_payment(tg_id: int, out_sum: float) -> int:
    async with db() as con:
        async with con.cursor() as cur:
            await cur.execute(
                "INSERT INTO payments(tg_id, out_sum, status, created_at) "
                "VALUES(%s,%s,%s,%s) RETURNING inv_id",
                (tg_id, float(out_sum), "created", now_ts()),
            )
            inv_id = (await cur.fetchone())["inv_id"]
        await con.commit()
    await upsert_user(tg_id, last_invoice_id=inv_id)
    return inv_id

async def set_payment_paid(inv_id: int):
    async with db() as con:
        async with con.cursor() as cur:
            await cur.execute(
                "UPDATE payments SET status='paid', paid_at=%s WHERE inv_id=%s",
                (now_ts(), inv_id),
            )
        await con.commit()

def pay_kb(url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💳 Оплатить {int(PRICE_RUB)} ₽ через Robokassa", url=url)]
    ])

# ======================= Документы / клавиатуры =======================
def legal_keyboard(token: str) -> InlineKeyboardMarkup:
    # Одна кнопка подтверждения (без обязательного открытия всех документов)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✔️ Подтвердить ознакомление", callback_data=f"legal_agree:{token}")]
    ])

def docs_keyboard(token: str) -> InlineKeyboardMarkup:
    # Ссылки на документы — по желанию пользователя
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Политика конфиденциальности", url=f"{BASE_URL}/policy/{token}")],
        [InlineKeyboardBut]()
