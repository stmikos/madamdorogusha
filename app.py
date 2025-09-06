# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
import os
import re
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from hashlib import md5, sha256
from textwrap import dedent
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    Update,
    ErrorEvent,
)

# ================= Common helpers =================
def now_ts() -> datetime:
    return datetime.now(timezone.utc)

def _clean(v: str | None) -> str:
    return (v or "").strip().strip('"').strip("'")

def money2(x) -> str:
    d = Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return format(d, ".2f")

# ================= logging =================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

# ================= env =================
load_dotenv()

# --- Bot / Webhook / Site
BOT_TOKEN = _clean(os.getenv("BOT_TOKEN"))
BASE_URL = _clean(os.getenv("BASE_URL")).rstrip("/")
WEBHOOK_SECRET = _clean(os.getenv("WEBHOOK_SECRET") or "secret")

# --- Channel / Admin
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0") or 0)
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0") or 0) or None

# --- Robokassa (гибкая поддержка алгоритма)
ROBOKASSA_LOGIN = _clean(os.getenv("ROBOKASSA_LOGIN"))
ROBOKASSA_PASSWORD1 = _clean(os.getenv("ROBOKASSA_PASSWORD1"))
ROBOKASSA_PASSWORD2 = _clean(os.getenv("ROBOKASSA_PASSWORD2"))

_raw_alg = (_clean(os.getenv("ROBOKASSA_SIGNATURE_ALG")) or "SHA256").upper().replace("-", "")
if _raw_alg in {"SHA256", "256"}:
    ROBOKASSA_SIGNATURE_ALG = "SHA256"
elif _raw_alg == "MD5":
    ROBOKASSA_SIGNATURE_ALG = "MD5"
else:
    raise RuntimeError(f"Invalid ROBOKASSA_SIGNATURE_ALG: {_raw_alg} (use MD5 or SHA256)")

ROBOKASSA_TEST_MODE = _clean(os.getenv("ROBOKASSA_TEST_MODE") or "0")  # "1" тест, "0" боевой
PRICE_RUB = Decimal(_clean(os.getenv("PRICE_RUB") or "10.00")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
SUBSCRIPTION_DAYS = int(os.getenv("SUBSCRIPTION_DAYS", "30"))

# --- DB env (Supabase Pooler friendly)
DB_HOST = _clean(os.getenv("DB_HOST")) or "aws-1-eu-central-1.pooler.supabase.com"
DB_PORT = int(os.getenv("DB_PORT", "6543"))
DB_NAME = _clean(os.getenv("DB_NAME") or "postgres")
DB_USER = _clean(os.getenv("DB_USER"))            # "postgres" ИЛИ "postgres.<project_ref>"
DB_PASSWORD = _clean(os.getenv("DB_PASSWORD"))
PROJECT_REF = _clean(os.getenv("PROJECT_REF"))    # например "vmwyfqsymxngrmdbwgbi"
DATABASE_URL = _clean(os.getenv("DATABASE_URL"))  # если задан — используется как основной путь

# ================= FastAPI & static =================
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

# ================= Aiogram (создаём ДО декораторов) =================
if not BOT_TOKEN or not BASE_URL:
    logger.warning("⚠️ BOT_TOKEN и/или BASE_URL не заданы — проверьте переменные окружения")
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

# ================= DB (Supabase Pooler) =================
def _log_db_cfg(eff_user: str, eff_options: str | None, using_url: bool):
    logger.info(
        "[DB CFG] host=%s port=%s db=%s user=%s sslmode=require options=%s (DATABASE_URL=%s)",
        DB_HOST, DB_PORT, DB_NAME, eff_user, eff_options, using_url
    )

def _compose_kw_from_env():
    """
    Маршрутизируемся через user=postgres.<project_ref>, без options.
    PROJECT_REF можно не задавать — берём из DB_USER, если он уже с суффиксом.
    """
    user = DB_USER or "postgres"

    # если пользователь без суффикса и PROJECT_REF задан — пришиваем его
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
    """
    Подключение к БД: используем только keyword args (без DATABASE_URL и без options).
    """
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

    return kw

def _strip_options_from_url(dsn: str):
    """
    Если в DATABASE_URL пришёл '...&options=project=xxx', вынимаем options и передадим отдельно,
    чтобы избежать 'extra key/value separator "=" in URI query parameter: "options"'.
    """
    try:
        pu = urlparse(dsn)
        q = dict(parse_qsl(pu.query, keep_blank_values=True))
        options_val = q.pop("options", None)
        new_query = urlencode(q)
        new_url = urlunparse((pu.scheme, pu.netloc, pu.path, pu.params, new_query, pu.fragment))
        return new_url, options_val
    except Exception:
        return dsn, None

@asynccontextmanager
async def db():
    """
    Подключение к БД.
    - Если задан DATABASE_URL, используем его (и аккуратно «вынем» options, если он в URL).
    - Иначе подключаемся по env, выбрав корректную схему с PROJECT_REF.
    """
    if DATABASE_URL:
        dsn, opt = _strip_options_from_url(DATABASE_URL)
        _log_db_cfg(DB_USER or "<url_user>", opt, using_url=True)
        if opt:
            conn = await psycopg.AsyncConnection.connect(
                dsn, options=opt, row_factory=dict_row, connect_timeout=10
            )
        else:
            conn = await psycopg.AsyncConnection.connect(
                dsn, row_factory=dict_row, connect_timeout=10
            )
        try:
            yield conn
        finally:
            await conn.close()
        return

    if not DB_USER or not DB_PASSWORD:
        raise RuntimeError("DB_USER/DB_PASSWORD must be set")

    kw = _compose_kw_from_env()
    _log_db_cfg(kw["user"], kw.get("options"), using_url=False)

    try:
        conn = await psycopg.AsyncConnection.connect(**kw)
    except psycopg.ProgrammingError as e:
        # fallback: если использовали options и получили tenant not found — пробуем user с суффиксом
        if "Tenant or user not found" in str(e) and kw.get("options") and PROJECT_REF:
            kw_fb = kw.copy()
            kw_fb.pop("options", None)
            kw_fb["user"] = f'{kw["user"]}.{PROJECT_REF}'
            logger.warning("[DB CFG] Fallback to user=%s without options", kw_fb["user"])
            conn = await psycopg.AsyncConnection.connect(**kw_fb)
        else:
            raise
    try:
        yield conn
    finally:
        await conn.close()

# ================= DB init & helpers =================
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
                        policy_viewed_at TIMESTAMPTZ,
                        consent_viewed_at TIMESTAMPTZ,
                        offer_viewed_at TIMESTAMPTZ,
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

                # журнал подтверждений
                await cur.execute(dedent("""
                    CREATE TABLE IF NOT EXISTS legal_confirms (
                        id BIGSERIAL PRIMARY KEY,
                        tg_id BIGINT,
                        token TEXT,
                        confirmed_at TIMESTAMPTZ DEFAULT now()
                    );
                """))
                await cur.execute("CREATE INDEX IF NOT EXISTS idx_legal_confirms_tg ON legal_confirms(tg_id);")

                # (необязательно) журнал просмотров документов
                await cur.execute(dedent("""
                    CREATE TABLE IF NOT EXISTS doc_views (
                        id BIGSERIAL PRIMARY KEY,
                        tg_id BIGINT,
                        token TEXT,
                        doc_type TEXT,
                        ip TEXT,
                        user_agent TEXT,
                        opened_at TIMESTAMPTZ DEFAULT now()
                    );
                """))
                await cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_views_token ON doc_views(token);")
                await cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_views_tg ON doc_views(tg_id);")

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

# ================= Robokassa =================
def _hash_hex(s: str) -> str:
    if ROBOKASSA_SIGNATURE_ALG == "SHA256":
        return sha256(s.encode("utf-8")).hexdigest().upper()
    return md5(s.encode("utf-8")).hexdigest().upper()

def sign_success(out_sum, inv_id: int) -> str:
    base = f"{ROBOKASSA_LOGIN}:{money2(out_sum)}:{inv_id}:{ROBOKASSA_PASSWORD1}"
    logger.info("RK base(success) %s", base.replace(ROBOKASSA_PASSWORD1, "***"))
    return _hash_hex(base)

def sign_result_from_raw(out_sum_str: str, inv_id: int) -> str:
    # OutSum берём как пришёл от Робокассы (строкой, без форматирования!)
    base = f"{out_sum_str}:{inv_id}:{ROBOKASSA_PASSWORD2}"
    logger.info("RK base(result) %s", base.replace(ROBOKASSA_PASSWORD2, "***"))
    return _hash_hex(base)

def build_pay_url(inv_id: int, out_sum, description: str = "Подписка на 30 дней") -> str:
    if not ROBOKASSA_LOGIN or not ROBOKASSA_PASSWORD1:
        missing = []
        if not ROBOKASSA_LOGIN:
            missing.append("ROBOKASSA_LOGIN")
        if not ROBOKASSA_PASSWORD1:
            missing.append("ROBOKASSA_PASSWORD1")
        raise RuntimeError(f"Robokassa credentials missing: {', '.join(missing)}")

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
    safe_log_params = {k: v for k, v in params.items() if k != "SignatureValue"}
    logger.info("[RK DEBUG] %s", safe_log_params)
    return url

async def new_payment(tg_id: int, out_sum) -> int:
    async with db() as con:
        async with con.cursor() as cur:
            await cur.execute(
                "INSERT INTO payments(tg_id, out_sum, status, created_at) VALUES(%s,%s,%s,%s) RETURNING inv_id",
                (tg_id, Decimal(str(out_sum)), "created", now_ts()),
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
        [InlineKeyboardButton(text=f"💳 Оплатить {money2(PRICE_RUB)} ₽ через Robokassa", url=url)]
    ])

# ================= Документы / подтверждение =================
def legal_keyboard(token: str) -> InlineKeyboardMarkup:
    # ОДНА кнопка подтверждения — по запросу пользователя
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✔️ Подтвердить ознакомление", callback_data=f"legal_agree:{token}")],
        [InlineKeyboardButton(text="📄 Документы", callback_data=f"open_docs:{token}")],
    ])

def docs_keyboard(token: str) -> InlineKeyboardMarkup:
    # Линки на документы — по желанию, без принуждения
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Политика конфиденциальности", url=f"{BASE_URL}/policy/{token}")],
        [InlineKeyboardButton(text="✅ Согласие на обработку данных", url=f"{BASE_URL}/consent/{token}")],
        [InlineKeyboardButton(text="📑 Публичная оферта", url=f"{BASE_URL}/offer/{token}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"back_to_legal:{token}")],
    ])

async def get_or_make_token(tg_id: int) -> str:
    u = await get_user(tg_id)
    if u and u.get("policy_token"):
        return u["policy_token"]
    token = secrets.token_urlsafe(16)
    await upsert_user(tg_id, policy_token=token, status="new", created_at=now_ts(), updated_at=now_ts())
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

# ================= Bot handlers =================
@dp.message(CommandStart())
async def on_start(message: Message):
    token = await get_or_make_token(message.from_user.id)
    txt = (
        "✨ Добро пожаловать!\n\n"
        "Нажмите «✔️ Подтвердить ознакомление», чтобы продолжить.\n"
        "Документы можно посмотреть по кнопке «📄 Документы»."
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
        "/stats — статус подписки\n"
        "/help — помощь"
    )

@dp.message(F.text == "📄 Документы")
@dp.message(Command("docs"))
async def on_docs(message: Message):
    token = await get_or_make_token(message.from_user.id)
    await message.answer("Документы:", reply_markup=docs_keyboard(token))

@dp.callback_query(F.data.startswith("open_docs:"))
async def on_open_docs(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]
    await cb.message.edit_text("Документы:", reply_markup=docs_keyboard(token))
    await cb.answer()

@dp.callback_query(F.data.startswith("back_to_legal:"))
async def on_back_to_legal(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]
    await cb.message.edit_text("Подтверждение:", reply_markup=legal_keyboard(token))
    await cb.answer()

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

    # фиксируем согласие + аудит
    async with db() as con:
        async with con.cursor() as cur:
            await cur.execute(
                "UPDATE users SET legal_confirmed_at=%s, status=%s, updated_at=%s WHERE tg_id=%s",
                (now_ts(), "legal_ok", now_ts(), tg_id),
            )
            await cur.execute(
                "INSERT INTO legal_confirms(tg_id, token, confirmed_at) VALUES (%s,%s,%s)",
                (tg_id, token, now_ts()),
            )
        await con.commit()

    # создаём платёж и ссылку
    inv_id = await new_payment(tg_id, PRICE_RUB)
    url = build_pay_url(inv_id, PRICE_RUB, "Подписка на 30 дней")

    await cb.message.answer("Спасибо! ✅ Теперь можно оплатить:", reply_markup=pay_kb(url))
    await cb.answer()

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

@dp.message(F.text == "💳 Оплатить подписку")
@dp.message(Command("pay"))
async def on_pay(message: Message):
    tg_id = message.from_user.id
    if not await _legal_ok(tg_id):
        token = await get_or_make_token(tg_id)
        await message.answer(
            "Сначала подтвердите ознакомление с условиями:",
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

@dp.message(F.text & ~F.text.regexp(r"^/"))
async def on_text(message: Message):
    await message.answer("Напишите /help для списка команд.")

# ================= Документные страницы (по желанию, фиксируют просмотр) =================
def _read_html(path: str) -> HTMLResponse:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("Файл не найден", status_code=404)

@app.get("/policy/{token}", response_class=HTMLResponse)
async def policy_with_token(token: str, request: Request):
    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent", "")
    try:
        async with db() as con:
            async with con.cursor() as cur:
                await cur.execute("UPDATE users SET policy_viewed_at=%s WHERE policy_token=%s", (now_ts(), token))
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
    return _read_html("static/policy.html")

@app.get("/consent/{token}", response_class=HTMLResponse)
async def consent_with_token(token: str, request: Request):
    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent", "")
    try:
        async with db() as con:
            async with con.cursor() as cur:
                await cur.execute("UPDATE users SET consent_viewed_at=%s WHERE policy_token=%s", (now_ts(), token))
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
    return _read_html("static/consent.html")

@app.get("/offer/{token}", response_class=HTMLResponse)
async def offer_with_token(token: str, request: Request):
    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent", "")
    try:
        async with db() as con:
            async with con.cursor() as cur:
                await cur.execute("UPDATE users SET offer_viewed_at=%s WHERE policy_token=%s", (now_ts(), token))
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
    return _read_html("static/offer.html")

# Plain (ручная проверка без фиксации)
@app.get("/policy", response_class=HTMLResponse)
def policy_plain():
    return _read_html("static/policy.html")

@app.get("/consent", response_class=HTMLResponse)
def consent_plain():
    return _read_html("static/consent.html")

@app.get("/offer", response_class=HTMLResponse)
def offer_plain():
    return _read_html("static/offer.html")

# ================= Robokassa callbacks =================
class RobokassaResult(BaseModel):
    OutSum: float
    InvId: int
    SignatureValue: str

def _eq_ci(a: str, b: str) -> bool:
    return (a or "").upper() == (b or "").upper()

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

    # даём инвайт в канал
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

# ================= Webhook & startup =================
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
    try:
        await init_db()
    except Exception as e:
        logger.error("startup init_db error: %s", e)

    # автосоздание html документов (на случай пустого деплоя)
    ensure("static/policy.html",
           "<!doctype html><meta charset='utf-8'><h1>Политика конфиденциальности</h1><p>Открытие фиксируется.</p>")
    ensure("static/consent.html",
           "<!doctype html><meta charset='utf-8'><h1>Согласие на обработку ПДн</h1><p>Открытие фиксируется.</p>")
    ensure("static/offer.html",
           "<!doctype html><meta charset='utf-8'><h1>Публичная оферта</h1><p>Открытие фиксируется.</p>")

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
