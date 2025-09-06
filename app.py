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
        async with db() as con:
            async with con.cursor() as cur:
                await cur.execute("SELECT legal_confirmed_at FROM users WHERE tg_id=%s", (tg_id,))
                r = await cur.fetchone()
        return bool(r and r.get("legal_confirmed_at"))
    except Exception as e:
        logger.error("_legal_ok failed: %s", e)
        return False

# ======================= Bot handlers =======================
@dp.message(CommandStart())
async def on_start(message: Message):
    token = await get_or_make_token(message.from_user.id)
    txt = (
        "👋 Добро пожаловать!\n\n"
        "Нажмите «✔️ Подтвердить ознакомление», затем оплатите подписку.\n"
        "Тексты документов доступны в меню «📄 Документы»."
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
        "/docs — ссылки на документы\n"
        "/pay — оплата (после подтверждения)\n"
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

    # находим пользователя по токену
    async with db() as con:
        async with con.cursor() as cur:
            await cur.execute("SELECT tg_id FROM users WHERE policy_token=%s", (token,))
            row = await cur.fetchone()

    if not row:
        await cb.answer("Сессия не найдена. Нажмите /start", show_alert=True)
        return

    tg_id = row["tg_id"]

    # фиксируем согласие + лог
    async with db() as con:
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

    # сразу создаём платёж и ссылку
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
        await message.answer("Сначала подтвердите ознакомление:", reply_markup=legal_keyboard(token))
        return

    try:
        inv_id = await new_payment(tg_id, PRICE_RUB)
        url = build_pay_url(inv_id, PRICE_RUB, "Подписка на 30 дней")
        await message.answer("Готово! Нажмите, чтобы оплатить:", reply_markup=pay_kb(url))
    except Exception as e:
        logger.error("/pay failed: %s", e)
        await message.answer("⚠️ Временно недоступно. Попробуйте позже.")

def bar(progress: float, width: int = 20) -> str:
    progress = max(0.0, min(1.0, float(progress)))
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
    progress = used / total if total.total_seconds() else 0
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

# ======================= Документные страницы (опционально) =======================
def _read_html_raw(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h2>Файл не найден</h2>"

@app.get("/policy/{token}", response_class=HTMLResponse)
async def policy_with_token(token: str):
    return HTMLResponse(_read_html_raw("static/policy.html"))

@app.get("/consent/{token}", response_class=HTMLResponse)
async def consent_with_token(token: str):
    return HTMLResponse(_read_html_raw("static/consent.html"))

@app.get("/offer/{token}", response_class=HTMLResponse)
async def offer_with_token(token: str):
    return HTMLResponse(_read_html_raw("static/offer.html"))

@app.get("/policy", response_class=HTMLResponse)
def policy_plain():
    return HTMLResponse(_read_html_raw("static/policy.html"))

@app.get("/consent", response_class=HTMLResponse)
def consent_plain():
    return HTMLResponse(_read_html_raw("static/consent.html"))

@app.get("/offer", response_class=HTMLResponse)
def offer_plain():
    return HTMLResponse(_read_html_raw("static/offer.html"))

# ======================= Robokassa callbacks =======================
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
        out_sum_raw = data.get("OutSum")  # строка из робокассы как есть
        inv_id = int(data.get("InvId"))
        sig = data.get("SignatureValue") or ""
    except Exception:
        raise HTTPException(400, "Bad payload")

    expected = sign_result_from_raw(out_sum_raw, inv_id)
    if not _eq_ci(sig, expected):
        try:
            async with db() as con:
                async with con.cursor() as cur:
                    await cur.execute("UPDATE payments SET status='failed' WHERE inv_id=%s", (inv_id,))
                await con.commit()
        except Exception:
            pass
        raise HTTPException(403, "Invalid signature")

    await set_payment_paid(inv_id)
    async with db() as con:
        async with con.cursor() as cur:
            await cur.execute("SELECT tg_id FROM payments WHERE inv_id=%s", (inv_id,))
            row = await cur.fetchone()

    if not row:
        return PlainTextResponse(f"OK{inv_id}")

    tg_id = row["tg_id"]
    valid_until = now_ts() + timedelta(days=SUBSCRIPTION_DAYS)
    await upsert_user(tg_id, status="active", valid_until=valid_until, remind_3d_sent=0)

    # выдаём инвайт в канал
    if bot and CHANNEL_ID:
        try:
            expire_at = now_ts() + timedelta(days=2)
            link = await bot.create_chat_invite_link(
                chat_id=CHANNEL_ID,
                name=f"Sub {tg_id} {inv_id}",
                expire_date=int(expire_at.timestamp()),
                member_limit=1
            )
            await bot.send_message(tg_id, f"Оплата получена ✅\nВаша ссылка в закрытый канал:\n{link.invite_link}")
        except Exception as e:
            logger.error("create_chat_invite_link failed: %s", e)
            if ADMIN_USER_ID and bot:
                try:
                    await bot.send_message(ADMIN_USER_ID, f"Не удалось создать инвайт: {e}")
                except Exception:
                    pass

    return PlainTextResponse(f"OK{inv_id}")

@app.get("/pay/success")
def pay_success():
    return HTMLResponse("<h2>Спасибо! Оплата прошла. Вернитесь в Telegram — приглашение уже ждёт вас в боте.</h2>")

@app.get("/pay/fail")
def pay_fail():
    return HTMLResponse("<h2>Оплата не завершена. Вы можете повторить попытку в боте.</h2>")

# ======================= Webhook & startup/shutdown =======================
@app.post(f"/telegram/webhook/{WEBHOOK_SECRET}")
async def telegram_webhook(request: Request):
    if not bot:
        raise HTTPException(500, "BOT_TOKEN не задан")
    data = await request.json()
    update = Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}

async def set_webhook():
    if not bot:
        logger.warning("BOT_TOKEN не задан — вебхук не установлен")
        return
    if not BASE_URL:
        logger.warning("BASE_URL не задан — вебхук не установлен")
        return
    await bot.set_webhook(f"{BASE_URL}/telegram/webhook/{WEBHOOK_SECRET}")

def ensure(path: str, content: str):
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

@app.on_event("startup")
async def startup():
    # файлы документов по умолчанию
    ensure("static/policy.html",
           "<!doctype html><meta charset='utf-8'><h1>Политика конфиденциальности</h1><p>Текст документа.</p>")
    ensure("static/consent.html",
           "<!doctype html><meta charset='utf-8'><h1>Согласие на обработку ПДн</h1><p>Текст документа.</p>")
    ensure("static/offer.html",
           "<!doctype html><meta charset='utf-8'><h1>Публичная оферта</h1><p>Текст документа.</p>")

    try:
        await init_db()
    except Exception as e:
        logger.error("startup init_db error: %s", e)

    try:
        await set_webhook()
    except Exception as e:
        logger.error("set_webhook failed: %s", e)

    async def loop():
        while True:
            await asyncio.sleep(3600)
    global loop_task
    loop_task = asyncio.create_task(loop())

@app.on_event("shutdown")
async def shutdown():
    global loop_task
    if loop_task:
        loop_task.cancel()
        try:
            await loop_task
        except asyncio.CancelledError:
            pass
    loop_task = None
