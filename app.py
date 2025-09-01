# -*- coding: utf-8 -*-
from textwrap import dedent
from psycopg.rows import dict_row
import os, re, asyncio, logging, secrets
from datetime import datetime, timedelta, timezone
from hashlib import md5, sha256
from urllib.parse import urlencode
from decimal import Decimal, ROUND_HALF_UP

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


# =============== utils ===============
def now_ts() -> datetime:
    return datetime.now(timezone.utc)


# =============== logging ===============
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")


# =============== env ===============
load_dotenv()

# Бот / вебхук / сайт
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BASE_URL = os.getenv("BASE_URL", "").strip().rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "Madamgorogusha")

# Канал / админ
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0") or 0)
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0") or 0) or None

# Robokassa
ROBOKASSA_LOGIN = os.getenv("ROBOKASSA_LOGIN", "").strip()
ROBOKASSA_PASSWORD1 = os.getenv("ROBOKASSA_PASSWORD1", "").strip()
ROBOKASSA_PASSWORD2 = os.getenv("ROBOKASSA_PASSWORD2", "").strip()
ROBOKASSA_SIGNATURE_ALG = os.getenv("ROBOKASSA_SIGNATURE_ALG", "SHA256").upper()  # MD5|SHA256
ROBOKASSA_TEST_MODE = os.getenv("ROBOKASSA_TEST_MODE", "0")  # "1" тест, "0" боевой

# читаем цену из .env, по умолчанию "10"
PRICE_RUB = Decimal(os.getenv("PRICE_RUB", "10")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def money2(x: Decimal) -> str:
    return format(x, ".2f")  # всегда '10.00'

SUBSCRIPTION_DAYS = int(os.getenv("SUBSCRIPTION_DAYS", "30"))

# БД: можно одной строкой или по полям (для Supabase pooler 6543)
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_HOST = os.getenv("DB_HOST", "").strip()
DB_PORT = int(os.getenv("DB_PORT", "6543"))
DB_NAME = os.getenv("DB_NAME", "postgres").strip()
DB_USER = os.getenv("DB_USER", "").strip()
DB_PASSWORD = os.getenv("DB_PASSWORD", "").strip()
PROJECT_REF = os.getenv("PROJECT_REF", "").strip()  # напр., ajcommzzdmzpyzzqclgb


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
    return {"status": "ok"}


# =============== Aiogram (создаём ДО декораторов) ===============
if not BOT_TOKEN or not BASE_URL:
    logger.warning("⚠️ BOT_TOKEN и/или BASE_URL не заданы — проверь .env")
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
    resize_keyboard=True
)


# =============== DB helpers ===============
async def db() -> psycopg.AsyncConnection:
    """
    Подключение к БД.
    Вариант 1: DATABASE_URL (полный DSN).
    Вариант 2: поля Supabase Pooler: host/port/db/user/pass + sslmode=require + options=project=REF.
    """
    if DATABASE_URL:
        return await psycopg.AsyncConnection.connect(
            DATABASE_URL, row_factory=dict_row, connect_timeout=10
        )

    host = DB_HOST or "aws-1-eu-north-1.pooler.supabase.com"
    port = int(DB_PORT or 6543)
    name = DB_NAME or "postgres"
    user = DB_USER
    pwd = DB_PASSWORD

    if not user or not pwd:
        raise RuntimeError("DB_USER/DB_PASSWORD не заданы в переменных окружения.")

    # Для Supabase pooler обязателен sslmode=require, а также options=project=PROJECT_REF
    dsn = f"host={host} port={port} dbname={name} user={user} password={pwd} sslmode=require"
    if PROJECT_REF:
        dsn += f" options=project={PROJECT_REF}"

        return await psycopg.AsyncConnection.connect(
        dsn, row_factory=dict_row, connect_timeout=10
    )

async def init_db():
    """Создаёт/мигрирует таблицы и индексы (идемпотентно)."""
    try:
                async with await db() as con:
            async with con.cursor() as cur:
                # users
                await cur.execute(dedent("""
                    CREATE TABLE IF NOT EXISTS users (
                        tg_id BIGINT PRIMARY KEY,
                        created_at TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ
                    );
                """))

                # добавляем недостающие колонки
                await cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT;")
                await cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS phone TEXT;")
                await cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'new';")
                await cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS policy_token TEXT;")
                await cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS policy_viewed_at TIMESTAMPTZ;")
                await cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS consent_viewed_at TIMESTAMPTZ;")
                await cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS offer_viewed_at TIMESTAMPTZ;")
                await cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS legal_confirmed_at TIMESTAMPTZ;")
                await cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS valid_until TIMESTAMPTZ;")
                await cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_invoice_id BIGINT;")
                await cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS remind_3d_sent INT DEFAULT 0;")
                await cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_pay_msg_id BIGINT;")
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

                # журнал просмотров документов
                await cur.execute(dedent("""
                    CREATE TABLE IF NOT EXISTS doc_views (
                        id BIGSERIAL PRIMARY KEY,
                        tg_id BIGINT,
                        token TEXT,
                        doc_type TEXT,   -- policy | consent | offer
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


# =============== Robokassa ===============
def _sign(s: str) -> str:
    return (
        sha256(s.encode()).hexdigest()
        if ROBOKASSA_SIGNATURE_ALG == "SHA256"
        else md5(s.encode()).hexdigest()
    ).upper()

def sign_success(out_sum: float, inv_id: int) -> str:
    base = f"{ROBOKASSA_LOGIN}:{money2(out_sum)}:{inv_id}:{ROBOKASSA_PASSWORD1}"
    return _sign(base)


def sign_result(out_sum: float, inv_id: int) -> str:
    base = f"{out_sum:.2f}:{inv_id}:{ROBOKASSA_PASSWORD2}"
    return _sign(base)


def build_pay_url(inv_id: int, out_sum: float, description: str) -> str:
    s = f"{ROBOKASSA_LOGIN}:{out_sum:.2f}:{inv_id}:{ROBOKASSA_PASSWORD1}"
    sig = _sign(s)
    
    params = {
        "MerchantLogin": ROBOKASSA_LOGIN,
        "OutSum": money2(out_sum),
        "InvId": str(inv_id),
        "Description": description,
        "SignatureValue": sig,
        "Culture": "ru",
        "Encoding": "utf-8",
        "IsTest": "0" if ROBOKASSA_TEST_MODE == "0" else "1",
        
    }
     url = "https://auth.robokassa.ru/Merchant/Index.aspx?" + urlencode(params)
    logger.info(
        "RK CHECK -> InvId=%s OutSum=%.2f base='%s' sig=%s url=%s",
        inv_id,
        out_sum,
        f"{ROBOKASSA_LOGIN}:{out_sum:.2f}:{inv_id}:***",
        sig,
        url,
    )
    return url


async def new_payment(tg_id: int, out_sum: float) -> int:
    async with await db() as con:
        async with con.cursor() as cur:
            await cur.execute(
                "INSERT INTO payments(tg_id, out_sum, status, created_at) VALUES(%s,%s,%s,%s) RETURNING inv_id",
                (tg_id, out_sum, "created", now_ts()),
            )
            inv_id = (await cur.fetchone())["inv_id"]
            await con.commit()
    await upsert_user(tg_id, last_invoice_id=inv_id)
    return inv_id


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


# =============== Документы ===============
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
    await_user(tg_id, policy_token=token, status="new")
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


# =============== Bot handlers ===============
@dp.message(CommandStart())
async def on_start(message: Message):
    token = await_or_make_token(message.from_user.id)
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
        "/stats — статус подписки\n"
        "/help — помощь"
    )


@dp.message(F.text == "📄 Документы")
@dp.message(Command("docs"))
async def on_docs(message: Message):
    token = await_or_make_token(message.from_user.id)
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

    # фиксируем согласие
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
    inv_id = await_payment(tg_id, PRICE_RUB)
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
        inv_id = await_payment(tg_id, PRICE_RUB)
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
    u = get_user(message.from_user.id)
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
    progress = min(max(used / total, 0), 1)
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


# =============== Документные страницы (фиксируют просмотр) ===============
def _read_html(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


@app.get("/policy/{token}", response_class=HTMLResponse)
async policy_with_token(token: str, request: Request):
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
async consent_with_token(token: str, request: Request):
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
async offer_with_token(token: str, request: Request):
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


# =============== Robokassa callbacks ===============
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
        out_sum = float(data.get("OutSum"))
        inv_id = int(data.get("InvId"))
        sig = data.get("SignatureValue")
    except Exception:
        raise HTTPException(400, "Bad payload")

    expected = sign_result(out_sum, inv_id)
    if not _eq_ci(sig, expected):
        try:
            async with await db() as con:
                async with con.cursor() as cur:
                    await cur.execute("UPDATE payments SET status='failed' WHERE inv_id=%s", (inv_id,))
                    await con.commit()
        except Exception:
            pass
        raise HTTPException(403, "Invalid signature")

    await set_payment_paid(inv_id)
    async with await db() as con:
        async with con.cursor() as cur:
            await cur.execute("SELECT tg_id FROM payments WHERE inv_id=%s", (inv_id,))
            row = await cur.fetchone()

    if not row:
        return PlainTextResponse(f"OK{inv_id}")

    tg_id = row["tg_id"]
    valid_until = now_ts() + timedelta(days=SUBSCRIPTION_DAYS)
    await_user(tg_id, status="active", valid_until=valid_until, remind_3d_sent=0)

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


# =============== Webhook & startup ===============
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
    # создаём/мигрируем БД
    try:
        await init_db()
    except Exception as e:
        logger.error("startup init_db error: %s", e)

    # автосоздание html-документов
    ensure("static/policy.html",
           "<!doctype html><meta charset='utf-8'><h1>Политика конфиденциальности</h1><p>Открытие фиксируется.</p>")
    ensure("static/consent.html",
           "<!doctype html><meta charset='utf-8'><h1>Согласие на обработку ПДн</h1><p>Открытие фиксируется.</p>")
    ensure("static/offer.html",
           "<!doctype html><meta charset='utf-8'><h1>Публичная оферта</h1><p>Открытие фиксируется.</p>")

    # выставляем вебхук
    try:
        await set_webhook()
    except Exception as e:
        logger.error("set_webhook failed: %s", e)

    # фоновой "живой" цикл
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
        finally:
            loop_task = None
