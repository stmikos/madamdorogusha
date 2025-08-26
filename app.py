# ===== imports =====
import os, re, asyncio, logging, secrets
from datetime import datetime, timedelta, timezone
from hashlib import md5, sha256
from urllib.parse import urlencode, quote

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
from psycopg.rows import dict_row


# ===== logging =====
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")


# ===== env =====
load_dotenv()

def _normalize_db_url(s: str | None) -> str | None:
    if not s:
        return s
    s = s.strip().strip('"').strip("'")
    # частая ошибка: вставляют целую строку "DATABASE_URL=postgresql://..."
    if s.upper().startswith("DATABASE_URL="):
        s = s.split("=", 1)[1].strip()
    if not re.search(r"[\?&]sslmode=", s, re.IGNORECASE):
        sep = "&" if "?" in s else "?"
        s = f"{s}{sep}sslmode=require"        
    return s

BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = (os.getenv("BASE_URL") or "").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "Madamgorogusha")  # можно задать в ENV
DATABASE_URL = _normalize_db_url(os.getenv("DATABASE_URL"))

CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0") or 0) or None

ROBOKASSA_LOGIN = os.getenv("ROBOKASSA_LOGIN", "")
ROBOKASSA_PASSWORD1 = os.getenv("ROBOKASSA_PASSWORD1", "")
ROBOKASSA_PASSWORD2 = os.getenv("ROBOKASSA_PASSWORD2", "")
ROBOKASSA_SIGNATURE_ALG = os.getenv("ROBOKASSA_SIGNATURE_ALG", "SHA256").upper()  # MD5|SHA256
ROBOKASSA_TEST_MODE = os.getenv("ROBOKASSA_TEST_MODE", "1")  # "1" тест, "0" боевой

PRICE_RUB = float(os.getenv("PRICE_RUB", "289"))
SUBSCRIPTION_DAYS = int(os.getenv("SUBSCRIPTION_DAYS", "30"))

if not BOT_TOKEN or not BASE_URL:
    raise RuntimeError("BOT_TOKEN и BASE_URL обязательны (BASE_URL без завершающего /).")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL обязателен (только сам URL, без 'DATABASE_URL=').")


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


# ===== Aiogram (инициализируем ДО декораторов) =====
bot = Bot(BOT_TOKEN)
dp = Dispatcher()

@dp.errors()
async def on_aiogram_error(event: ErrorEvent):
    logger.exception("Aiogram handler error", exc_info=event.exception)
    if ADMIN_USER_ID:
        try:
            await bot.send_message(
                ADMIN_USER_ID,
                f"⚠️ Ошибка: {type(event.exception).__name__}\n{event.exception}"
            )
        except Exception:
            pass
    return True


# ===== UI =====
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


# ===== DB helpers =====
def db():
    # connect_timeout, чтобы быстро падать при сетевых проблемах
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=10)

def now_ts():
    return datetime.now(timezone.utc)

def init_db():
    try:
        with db() as con, con.cursor() as cur:
            # users
            cur.execute("""
            CREATE TABLE IF NOT EXISTS users(
                tg_id BIGINT PRIMARY KEY,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ
            );
            """)
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS phone TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'new';")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS policy_token TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS policy_viewed_at TIMESTAMPTZ;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS consent_viewed_at TIMESTAMPTZ;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS offer_viewed_at TIMESTAMPTZ;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS legal_confirmed_at TIMESTAMPTZ;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS valid_until TIMESTAMPTZ;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_invoice_id BIGINT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS remind_3d_sent INT DEFAULT 0;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_pay_msg_id BIGINT;")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);")

            # payments
            cur.execute("""
            CREATE TABLE IF NOT EXISTS payments(
                inv_id BIGSERIAL PRIMARY KEY,
                tg_id BIGINT,
                out_sum NUMERIC(12,2),
                status TEXT,
                created_at TIMESTAMPTZ,
                paid_at TIMESTAMPTZ,
                signature TEXT
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_tg ON payments(tg_id);")
            con.commit()
        logger.info("init_db: OK")
    except Exception as e:
        logger.error(f"init_db failed: {e}")
        raise

def get_user(tg_id: int):
    try:
        with db() as con, con.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE tg_id=%s", (tg_id,))
            return cur.fetchone()
    except Exception as e:
        logger.error(f"get_user error: {e}")
        return None

def upsert_user(tg_id: int, **kwargs):
    try:
        reserved = {"tg_id", "created_at", "updated_at"}
        data = {k: v for k, v in kwargs.items() if k not in reserved}
        row = get_user(tg_id)
        if row is None:
            cols = ["tg_id", "created_at", "updated_at"] + list(data.keys())
            vals = [tg_id, now_ts(), now_ts()] + list(data.values())
            ph = ["%s"] * len(cols)
            with db() as con, con.cursor() as cur:
                cur.execute(f"INSERT INTO users({','.join(cols)}) VALUES({','.join(ph)})", tuple(vals))
        else:
            if data:
                sets = [f"{k}=%s" for k in data] + ["updated_at=%s"]
                vals = list(data.values()) + [now_ts(), tg_id]
                with db() as con, con.cursor() as cur:
                    cur.execute(f"UPDATE users SET {', '.join(sets)} WHERE tg_id=%s", tuple(vals))
            else:
                with db() as con, con.cursor() as cur:
                    cur.execute("UPDATE users SET updated_at=%s WHERE tg_id=%s", (now_ts(), tg_id))
    except Exception as e:
        logger.error(f"upsert_user error: {e}")

def list_active_users():
    try:
        with db() as con, con.cursor() as cur:
            cur.execute("SELECT tg_id, valid_until, remind_3d_sent FROM users WHERE status='active' AND valid_until IS NOT NULL")
            return cur.fetchall()
    except Exception as e:
        logger.error(f"list_active_users error: {e}")
        return []


# ===== Robokassa =====
def _sign(s: str) -> str:
    return sha256(s.encode()).hexdigest() if ROBOKASSA_SIGNATURE_ALG == "SHA256" else md5(s.encode()).hexdigest()

def sign_success(out_sum: float, inv_id: int) -> str:
    base = f"{ROBOKASSA_LOGIN}:{out_sum:.2f}:{inv_id}:{ROBOKASSA_PASSWORD1}"
    return _sign(base)

def sign_result(out_sum: float, inv_id: int) -> str:
    base = f"{out_sum:.2f}:{inv_id}:{ROBOKASSA_PASSWORD2}"
    return _sign(base)

def build_pay_url(inv_id: int, out_sum: float, description: str = "Подписка на 30 дней") -> str:
    params = {
        "MerchantLogin": ROBOKASSA_LOGIN,
        "OutSum": f"{out_sum:.2f}",
        "InvId": str(inv_id),
        "Description": quote(description, safe=""),
        "SignatureValue": sign_success(out_sum, inv_id),
        "Culture": "ru",
        "Encoding": "utf-8",
    }
    # В тестовом режиме показываем тестовую оплату
    params["IsTest"] = "1" if ROBOKASSA_TEST_MODE == "0" else "0"
    # (опционально) можно задать success/fail прямо в ссылке:
    params["SuccessURL"] = f"{BASE_URL}/pay/success"
    params["FailURL"]    = f"{BASE_URL}/pay/fail"
    url = "https://auth.robokassa.ru/Merchant/Index.aspx?" + urlencode(params)
    logger.info("[RK DEBUG] " + str(params))
    return url

def new_payment(tg_id: int, out_sum: float) -> int:
    with db() as con, con.cursor() as cur:
        cur.execute(
            "INSERT INTO payments(tg_id, out_sum, status, created_at) VALUES(%s,%s,%s,%s) RETURNING inv_id",
            (tg_id, out_sum, "created", now_ts())
        )
        inv_id = cur.fetchone()["inv_id"]
    upsert_user(tg_id, last_invoice_id=inv_id)
    return inv_id

def set_payment_paid(inv_id: int):
    with db() as con, con.cursor() as cur:
        cur.execute("UPDATE payments SET status='paid', paid_at=%s WHERE inv_id=%s", (now_ts(), inv_id))


def pay_kb(url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💳 Оплатить {int(PRICE_RUB)} ₽ через Robokassa", url=url)]
    ])


# ===== Документы =====
def legal_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Политика конфиденциальности", url=f"{BASE_URL}/policy/{token}")],
        [InlineKeyboardButton(text="✅ Согласие на обработку данных",   url=f"{BASE_URL}/consent/{token}")],
        [InlineKeyboardButton(text="📑 Публичная оферта",               url=f"{BASE_URL}/offer/{token}")],
        [InlineKeyboardButton(text="✔️ Я ознакомился(лась)", callback_data=f"legal_agree:{token}")],
    ])

def get_or_make_token(tg_id: int) -> str:
    u = get_user(tg_id)
    if u and u.get("policy_token"):
        return u["policy_token"]
    token = secrets.token_urlsafe(16)
    upsert_user(tg_id, policy_token=token, status="new")
    return token

def _legal_ok(tg_id: int) -> bool:
    try:
        with db() as con, con.cursor() as cur:
            cur.execute("SELECT legal_confirmed_at FROM users WHERE tg_id=%s", (tg_id,))
            r = cur.fetchone()
        return bool(r and r.get("legal_confirmed_at"))
    except Exception as e:
        logger.error(f"_legal_ok error: {e}")
        return False


# ======= Bot handlers =======
@dp.message(CommandStart())
async def on_start(message: Message):
    tg_id = message.from_user.id
    token = get_or_make_token(tg_id)
    txt = (
        "👋 Добро пожаловать!\n\n"
        "1) Откройте три документа ниже\n"
        "2) Нажмите «✔️ Я ознакомился(лась)»\n"
        "3) Затем оплатите подписку"
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
    token = get_or_make_token(message.from_user.id)
    await message.answer("Документы:", reply_markup=legal_keyboard(token))

@dp.callback_query(F.data.startswith("legal_agree:"))
async def on_legal_agree(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]
    # 1) Находим пользователя по токену
    with db() as con, con.cursor() as cur:
        cur.execute("""
            SELECT tg_id, policy_viewed_at, consent_viewed_at, offer_viewed_at
            FROM users WHERE policy_token=%s
        """, (token,))
        row = cur.fetchone()
    if not row:
        await cb.answer("Сессия не найдена. Нажмите /start", show_alert=True)
        return
    # 2) Проверяем отметки
    if not (row.get("policy_viewed_at") and row.get("consent_viewed_at") and row.get("offer_viewed_at")):
        await cb.answer("Пожалуйста, откройте все три документа (Политика, Согласие, Оферта).", show_alert=True)
        return
    # 3) Фиксируем согласие
    with db() as con, con.cursor() as cur:
        cur.execute("UPDATE users SET legal_confirmed_at=%s, status=%s WHERE tg_id=%s",
                    (now_ts(), "legal_ok", row["tg_id"]))
    # 4) Сразу даём оплату
    inv_id = new_payment(row["tg_id"], PRICE_RUB)
    url = build_pay_url(inv_id, PRICE_RUB, "Подписка на 30 дней")
    await cb.message.answer("Спасибо! ✅ Теперь можно оплатить:", reply_markup=pay_kb(url))
    await cb.answer()

@dp.message(F.text == "💳 Оплатить подписку")
@dp.message(Command("pay"))
async def on_pay(message: Message):
    tg_id = message.from_user.id
    if not _legal_ok(tg_id):
        token = get_or_make_token(tg_id)
        await message.answer("Сначала ознакомьтесь с документами и подтвердите согласие:", reply_markup=legal_keyboard(token))
        return

    # удалить предыдущее «оплатное» сообщение (если было)
    u = get_user(tg_id)
    if u and u.get("last_pay_msg_id"):
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=u["last_pay_msg_id"])
        except Exception:
            pass

    inv_id = new_payment(tg_id, PRICE_RUB)
    url = build_pay_url(inv_id, PRICE_RUB, "Подписка на 30 дней")
    m = await message.answer("Готово! Нажмите, чтобы оплатить:", reply_markup=pay_kb(url))
    upsert_user(tg_id, last_pay_msg_id=m.message_id)

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

# НЕ перехватываем команды
@dp.message(F.text & ~F.text.regexp(r"^/"))
async def on_text(message: Message):
    await message.answer("Напишите /help для списка команд.")


# ======= Документные страницы (фиксируют просмотр) =======
def _read_html(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

@app.get("/policy/{token}", response_class=HTMLResponse)
def policy_with_token(token: str):
    try:
        with db() as con, con.cursor() as cur:
            cur.execute("UPDATE users SET policy_viewed_at=%s WHERE policy_token=%s", (now_ts(), token))
    except Exception as e:
        logger.error(f"policy update failed: {e}")
    return HTMLResponse(_read_html("static/policy.html"))

@app.get("/consent/{token}", response_class=HTMLResponse)
def consent_with_token(token: str):
    try:
        with db() as con, con.cursor() as cur:
            cur.execute("UPDATE users SET consent_viewed_at=%s WHERE policy_token=%s", (now_ts(), token))
    except Exception as e:
        logger.error(f"consent update failed: {e}")
    return HTMLResponse(_read_html("static/consent.html"))

@app.get("/offer/{token}", response_class=HTMLResponse)
def offer_with_token(token: str):
    try:
        with db() as con, con.cursor() as cur:
            cur.execute("UPDATE users SET offer_viewed_at=%s WHERE policy_token=%s", (now_ts(), token))
    except Exception as e:
        logger.error(f"offer update failed: {e}")
    return HTMLResponse(_read_html("static/offer.html"))

# Plain-страницы для ручной проверки (без фиксации)
@app.get("/policy", response_class=HTMLResponse)
def policy_plain():  return HTMLResponse(_read_html("static/policy.html"))
@app.get("/consent", response_class=HTMLResponse)
def consent_plain(): return HTMLResponse(_read_html("static/consent.html"))
@app.get("/offer", response_class=HTMLResponse)
def offer_plain():   return HTMLResponse(_read_html("static/offer.html"))


# ======= Robokassa callbacks =======
class RobokassaResult(BaseModel):
    OutSum: float
    InvId: int
    SignatureValue: str

def _eq_ci(a: str, b: str) -> bool: return (a or "").lower() == (b or "").lower()

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
        with db() as con, con.cursor() as cur:
            cur.execute("UPDATE payments SET status='failed' WHERE inv_id=%s", (inv_id,))
        raise HTTPException(403, "Invalid signature")

    set_payment_paid(inv_id)
    with db() as con, con.cursor() as cur:
        cur.execute("SELECT tg_id FROM payments WHERE inv_id=%s", (inv_id,))
        row = cur.fetchone()
    if not row:
        return PlainTextResponse(f"OK{inv_id}")
    tg_id = row["tg_id"]

    valid_until = now_ts() + timedelta(days=SUBSCRIPTION_DAYS)
    upsert_user(tg_id, status="active", valid_until=valid_until, remind_3d_sent=0)

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
        if ADMIN_USER_ID:
            await bot.send_message(ADMIN_USER_ID, f"Не удалось создать инвайт: {e}")

    return PlainTextResponse(f"OK{inv_id}")

@app.get("/pay/success")
def pay_success():
    return HTMLResponse("<h2>Спасибо! Оплата прошла. Вернитесь в Telegram — приглашение уже ждёт вас в боте.</h2>")

@app.get("/pay/fail")
def pay_fail():
    return HTMLResponse("<h2>Оплата не завершена. Вы можете повторить попытку в боте.</h2>")


# ======= Webhook & startup =======
@app.post(f"/telegram/webhook/{WEBHOOK_SECRET}")
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}

async def set_webhook():
    await bot.set_webhook(f"{BASE_URL}/telegram/webhook/{WEBHOOK_SECRET}", drop_pending_updates=True)

def ensure(path: str, content: str):
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

@app.on_event("startup")
async def startup():
    # Автосоздание файлов документов
    ensure(
        "static/policy.html",
        """<!doctype html><meta charset="utf-8"><h1>Политика конфиденциальности</h1><p>Открытие фиксируется.</p>"""
    )
    ensure(
        "static/consent.html",
        """<!doctype html><meta charset="utf-8"><h1>Согласие на обработку ПДн</h1><p>Открытие фиксируется.</p>"""
    )
    ensure(
        "static/offer.html",
        """<!doctype html><meta charset="utf-8"><h1>Публичная оферта</h1><p>Открытие фиксируется.</p>"""
    )

    init_db()
    try:
        await set_webhook()
    except Exception as e:
        logger.error(f"set_webhook failed: {e}")

    async def bg_loop():
        while True:
            await asyncio.sleep(3600)
    asyncio.create_task(bg_loop())
