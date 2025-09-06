# -*- coding: utf-8 -*-
from textwrap import dedent
from psycopg.rows import dict_row
from contextlib import asynccontextmanager
from decimal import Decimal, ROUND_HALF_UP
import os, re, asyncio, logging, secrets
from datetime import datetime, timedelta, timezone
from hashlib import md5, sha256
from urllib.parse import urlencode

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
    FSInputFile, ErrorEvent
)

import psycopg


# ===== util =====
def now_ts() -> datetime:
    return datetime.now(timezone.utc)

def money2(x) -> str:
    d = Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return format(d, ".2f")

def _clean(v: str | None) -> str:
    return (v or "").strip().strip('"').strip("'")


# ===== logging & env =====
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

load_dotenv()

# Бот / вебхук / сайт
BOT_TOKEN      = _clean(os.getenv("BOT_TOKEN"))
BASE_URL       = _clean(os.getenv("BASE_URL")).rstrip("/")
WEBHOOK_SECRET = _clean(os.getenv("WEBHOOK_SECRET") or "secret")

# Канал / админ
CHANNEL_ID    = int(os.getenv("CHANNEL_ID", "0") or 0)
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0") or 0) or None

# Robokassa
ROBOKASSA_LOGIN          = _clean(os.getenv("ROBOKASSA_LOGIN"))
ROBOKASSA_PASSWORD1      = _clean(os.getenv("ROBOKASSA_PASSWORD1"))          # боевой
ROBOKASSA_PASSWORD2      = _clean(os.getenv("ROBOKASSA_PASSWORD2"))          # боевой
ROBOKASSA_PASSWORD1_TEST = _clean(os.getenv("ROBOKASSA_PASSWORD1_TEST"))     # тестовый (опц.)
ROBOKASSA_PASSWORD2_TEST = _clean(os.getenv("ROBOKASSA_PASSWORD2_TEST"))     # тестовый (опц.)
ROBOKASSA_SIGNATURE_ALG  = (_clean(os.getenv("ROBOKASSA_SIGNATURE_ALG")) or "SHA256").upper()  # SHA256|MD5
ROBOKASSA_TEST_MODE      = (_clean(os.getenv("ROBOKASSA_TEST_MODE")) or "0")  # "1" тест, "0" боевой

PRICE_RUB         = Decimal(_clean(os.getenv("PRICE_RUB") or "10")).quantize(Decimal("0.01"))
SUBSCRIPTION_DAYS = int(os.getenv("SUBSCRIPTION_DAYS", "30"))

# БД (Supabase Pooler 6543 или полный DATABASE_URL)
DATABASE_URL = _clean(os.getenv("DATABASE_URL"))
DB_HOST      = _clean(os.getenv("DB_HOST"))
DB_PORT      = int(os.getenv("DB_PORT", "6543"))
DB_NAME      = _clean(os.getenv("DB_NAME") or "postgres")
DB_USER      = _clean(os.getenv("DB_USER"))
DB_PASSWORD  = _clean(os.getenv("DB_PASSWORD"))
PROJECT_REF  = _clean(os.getenv("PROJECT_REF"))  # напр. vmwyfqsymxngrmdbwgbi


# ===== FastAPI & static =====
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


# ===== Aiogram (создаём ДО декораторов) =====
if not BOT_TOKEN or not BASE_URL:
    logger.warning("⚠️ BOT_TOKEN и/или BASE_URL не заданы — проверь переменные окружения.")
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


# ===== DB: аккуратный коннект к Supabase =====
def _compose_conninfo() -> tuple[str, dict]:
    """
    Возвращает (conninfo_or_url, kwargs) для psycopg.AsyncConnection.connect.
    - Если задан DATABASE_URL: используем его + options через kwargs (без '?options=...' в URL!).
    - Иначе собираем conninfo-строку host=... port=... dbname=... user=... password=... sslmode=require [options=project=REF]
    """
    kwargs = dict(row_factory=dict_row, connect_timeout=10)
    if DATABASE_URL:
        if PROJECT_REF:
            kwargs["options"] = f"project={PROJECT_REF}"
        return DATABASE_URL, kwargs

    host = DB_HOST or "aws-1-eu-north-1.pooler.supabase.com"
    port = DB_PORT or 6543
    name = DB_NAME or "postgres"
    user = DB_USER
    pwd  = DB_PASSWORD
    if not user or not pwd:
        raise RuntimeError("DB_USER/DB_PASSWORD must be set")

    conninfo = f"host={host} port={port} dbname={name} user={user} password={pwd} sslmode=require"
    if PROJECT_REF:
        conninfo += f" options=project={PROJECT_REF}"
    return conninfo, kwargs

@asynccontextmanager
async def db():
    conninfo, kwargs = _compose_conninfo()
    logger.info("[DB CFG] host=%s port=%s db=%s user=%s sslmode=require options=project=%s (DATABASE_URL=%s)",
                DB_HOST or "aws-1-eu-central-1.pooler.supabase.com",
                DB_PORT or 6543,
                DB_NAME or "postgres",
                (DB_USER or (DATABASE_URL and "<url-user>") or ""),
                (PROJECT_REF or ""),
                bool(DATABASE_URL))
    conn = await psycopg.AsyncConnection.connect(conninfo, **kwargs)
    try:
        yield conn
    finally:
        await conn.close()


async def init_db():
    try:
        async with db() as con:
            async with con.cursor() as cur:
                # --- schema: users ---
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        tg_id BIGINT PRIMARY KEY,
                        created_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now(),
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
                """)
                await cur.execute("CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);")

                # --- schema: payments ---
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS payments (
                        inv_id BIGSERIAL PRIMARY KEY,
                        tg_id BIGINT,
                        out_sum NUMERIC(12,2),
                        status TEXT,                 -- created | paid | failed
                        created_at TIMESTAMPTZ,
                        paid_at TIMESTAMPTZ,
                        signature TEXT
                    );
                """)
                await cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_tg ON payments(tg_id);")

                # --- schema: legal_confirms (аудит подтверждений) ---
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS legal_confirms (
                        id BIGSERIAL PRIMARY KEY,
                        tg_id BIGINT,
                        confirmed_at TIMESTAMPTZ DEFAULT now()
                    );
                """)
                await cur.execute("CREATE INDEX IF NOT EXISTS idx_lc_tg ON legal_confirms(tg_id);")

                # --- cleanup duplicates before unique index ---
                await cur.execute("""
                    WITH ranked AS (
                        SELECT inv_id, tg_id,
                               row_number() OVER (PARTITION BY tg_id ORDER BY inv_id DESC) AS rn
                        FROM payments
                        WHERE status = 'created'
                    )
                    UPDATE payments p
                    SET status = 'failed'
                    FROM ranked r
                    WHERE p.inv_id = r.inv_id
                      AND r.rn > 1;
                """)

                # --- unique partial index ---
                await cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS uniq_open_invoice_per_user
                    ON payments(tg_id)
                    WHERE status = 'created';
                """)

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
                    FROM users
                    WHERE status='active' AND valid_until IS NOT NULL
                """)
                return await cur.fetchall()
    except Exception as e:
        logger.error("list_active_users failed: %s", e)
        return []


# ===== Robokassa =====
def _hash_hex(s: str) -> str:
    if ROBOKASSA_SIGNATURE_ALG == "SHA256":
        return sha256(s.encode("utf-8")).hexdigest().upper()
    elif ROBOKASSA_SIGNATURE_ALG == "MD5":
        return md5(s.encode("utf-8")).hexdigest().upper()
    raise RuntimeError(f"Unsupported ROBOKASSA_SIGNATURE_ALG={ROBOKASSA_SIGNATURE_ALG}")

def _pwd1() -> str:
    return ROBOKASSA_PASSWORD1_TEST if ROBOKASSA_TEST_MODE == "1" else ROBOKASSA_PASSWORD1

def _pwd2() -> str:
    return ROBOKASSA_PASSWORD2_TEST if ROBOKASSA_TEST_MODE == "1" else ROBOKASSA_PASSWORD2

def sign_success(out_sum, inv_id: int) -> str:
    # MerchantLogin:OutSum:InvId:Password1
    base = f"{ROBOKASSA_LOGIN}:{money2(out_sum)}:{inv_id}:{_pwd1()}"
    logger.info("RK base(success)='%s'", base.replace(_pwd1(), "***"))
    return _hash_hex(base)

def sign_result_from_raw(out_sum_raw: str, inv_id: int) -> str:
    # OutSum(КАК ПРИШЛА СТРОКОЙ):InvId:Password2
    base = f"{out_sum_raw}:{inv_id}:{_pwd2()}"
    logger.info("RK base(result)='%s'", base.replace(_pwd2(), "***"))
    return _hash_hex(base)

def build_pay_url(inv_id: int, out_sum, description: str = "Подписка на 30 дней") -> str:
    if not ROBOKASSA_LOGIN or not _pwd1():
        raise RuntimeError("Robokassa login/password1 not set")
    params = {
        "MerchantLogin":  ROBOKASSA_LOGIN,
        "OutSum":         money2(out_sum),
        "InvId":          str(inv_id),
        "Description":    description,
        "SignatureValue": sign_success(out_sum, inv_id),
        "Culture":        "ru",
        "Encoding":       "utf-8",
        "IsTest":         "0" if ROBOKASSA_TEST_MODE == "0" else "0",
    }
    url = "https://auth.robokassa.ru/Merchant/Index.aspx?" + urlencode(params)
    logger.info("[RK DEBUG] %s", {k: v for k, v in params.items() if k != "SignatureValue"})
    return url


async def ensure_open_invoice(tg_id: int, out_sum) -> int:
    """
    Возвращает существующий 'created' inv_id для пользователя, иначе создаёт новый.
    Гарантируется уникальным индексом uniq_open_invoice_per_user.
    """
    async with db() as con:
        async with con.cursor() as cur:
            await cur.execute(
                "SELECT inv_id FROM payments WHERE tg_id=%s AND status='created' ORDER BY inv_id DESC LIMIT 1",
                (tg_id,),
            )
            row = await cur.fetchone()
            if row:
                return row["inv_id"]

            await cur.execute(
                "INSERT INTO payments(tg_id, out_sum, status, created_at) VALUES(%s,%s,%s,%s) RETURNING inv_id",
                (tg_id, Decimal(money2(out_sum)), "created", now_ts()),
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


# ===== Документы / Согласие =====
def legal_keyboard(token: str) -> InlineKeyboardMarkup:
    # ЕДИНАЯ кнопка подтверждения
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✔️ Подтвердить ознакомление", callback_data=f"legal_agree:{token}")],
    ])

def docs_keyboard(token: str) -> InlineKeyboardMarkup:
    # Документы доступны, но подтверждение — отдельной кнопкой
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


# ===== Bot handlers =====
@dp.message(CommandStart())
async def on_start(message: Message):
    token = await get_or_make_token(message.from_user.id)
    txt = (
        "👋 Добро пожаловать!\n\n"
        "1) Нажмите «✔️ Подтвердить ознакомление»\n"
        "2) Затем оплатите подписку\n\n"
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
        "/docs — документы\n"
        "/pay — оплата (после согласия)\n"
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

    # Найдём пользователя по токену
    async with db() as con:
        async with con.cursor() as cur:
            await cur.execute("SELECT tg_id FROM users WHERE policy_token=%s", (token,))
            row = await cur.fetchone()

    if not row:
        await cb.answer("Сессия не найдена. Нажмите /start", show_alert=True)
        return

    tg_id = row["tg_id"]

    # Фиксируем согласие + аудит
    async with db() as con:
        async with con.cursor() as cur:
            await cur.execute(
                "UPDATE users SET legal_confirmed_at=%s, status=%s WHERE tg_id=%s",
                (now_ts(), "legal_ok", tg_id),
            )
            await cur.execute(
                "INSERT INTO legal_confirms(tg_id, confirmed_at) VALUES (%s,%s)",
                (tg_id, now_ts()),
            )
        await con.commit()

    # Готовим/возвращаем открытый счёт
    inv_id = await ensure_open_invoice(tg_id, PRICE_RUB)
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
        inv_id = await ensure_open_invoice(tg_id, PRICE_RUB)
        url = build_pay_url(inv_id, PRICE_RUB, "Подписка на 30 дней")
        await message.answer("Готово! Нажмите, чтобы оплатить:", reply_markup=pay_kb(url))
    except Exception as e:
        logger.error("/pay failed: %s", e)
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


# ===== Документные страницы (по желанию, без обязательства открывать) =====
def _read_html(path: str) -> HTMLResponse:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("Файл не найден.", status_code=404)

@app.get("/policy/{token}", response_class=HTMLResponse)
async def policy_with_token(token: str):
    return _read_html("static/policy.html")

@app.get("/consent/{token}", response_class=HTMLResponse)
async def consent_with_token(token: str):
    return _read_html("static/consent.html")

@app.get("/offer/{token}", response_class=HTMLResponse)
async def offer_with_token(token: str):
    return _read_html("static/offer.html")

# Plain
@app.get("/policy", response_class=HTMLResponse)
def policy_plain():
    return _read_html("static/policy.html")

@app.get("/consent", response_class=HTMLResponse)
def consent_plain():
    return _read_html("static/consent.html")

@app.get("/offer", response_class=HTMLResponse)
def offer_plain():
    return _read_html("static/offer.html")


# ===== Robokassa callbacks =====
class RobokassaResult(BaseModel):
    OutSum: float
    InvId: int
    SignatureValue: str

@app.post("/pay/result")
async def pay_result(request: Request):
    data = dict(await request.form())
    try:
        out_sum_raw = data.get("OutSum")  # как пришла строкой!
        inv_id = int(data.get("InvId"))
        sig = (data.get("SignatureValue") or "").strip()
    except Exception:
        raise HTTPException(400, "Bad payload")

    expected = sign_result_from_raw(out_sum_raw, inv_id)
    if (sig or "").upper() != expected.upper():
        # пометим инвойс как failed (если был)
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

    # Пытаемся выдать инвайт в канал
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


# ===== Webhook & startup =====
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
    # автосоздание html-документов
    ensure("static/policy.html",
           "<!doctype html><meta charset='utf-8'><h1>Политика конфиденциальности</h1><p>Текст политики…</p>")
    ensure("static/consent.html",
           "<!doctype html><meta charset='utf-8'><h1>Согласие на обработку ПДн</h1><p>Текст согласия…</p>")
    ensure("static/offer.html",
           "<!doctype html><meta charset='utf-8'><h1>Публичная оферта</h1><p>Текст оферты…</p>")

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
