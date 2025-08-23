import os
import re
import asyncio
import secrets
from datetime import datetime, timedelta, timezone
from hashlib import md5, sha256
from urllib.parse import urlencode, quote

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    FSInputFile, Update,
)

# Postgres
import psycopg
from psycopg.rows import dict_row

# =================== ENV ===================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", secrets.token_urlsafe(16))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0")) if os.getenv("ADMIN_USER_ID") else None

ROBOKASSA_LOGIN = os.getenv("ROBOKASSA_LOGIN", "")
ROBOKASSA_PASSWORD1 = os.getenv("ROBOKASSA_PASSWORD1", "")
ROBOKASSA_PASSWORD2 = os.getenv("ROBOKASSA_PASSWORD2", "")
ROBOKASSA_SIGNATURE_ALG = os.getenv("ROBOKASSA_SIGNATURE_ALG", "MD5").upper()
ROBOKASSA_TEST_MODE = int(os.getenv("ROBOKASSA_TEST_MODE", "0"))

PRICE_RUB = float(os.getenv("PRICE_RUB", "289"))
SUBSCRIPTION_DAYS = int(os.getenv("SUBSCRIPTION_DAYS", "30"))

DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN or not BASE_URL:
    raise RuntimeError("BOT_TOKEN и BASE_URL обязательны (BASE_URL — публичный адрес сервиса).")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL обязателен (строка подключения к Supabase Postgres, со строкой ?sslmode=require).")

# =================== TG bot ===================
bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# =================== FastAPI ===================
app = FastAPI(title="Telegram Subscription Bot (Supabase/Postgres)")

@app.get("/health")
def health():
    return {"status": "ok"}
# сразу после импортов, ДО app.mount:
import os
os.makedirs("static", exist_ok=True)
os.makedirs("assets", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

app.mount("/static", StaticFiles(directory="static"), name="static")
from fastapi.responses import HTMLResponse

@app.get("/", response_class=HTMLResponse)
def root():
    return HTMLResponse("<h3>OK: бот работает. /health тоже OK. Политика по кнопке в боте.</h3>")
 
@app.get("/policy-test", response_class=HTMLResponse)
def policy_test():
    path = "static/policy.html"
    if not os.path.exists(path):
        return HTMLResponse("<h2>static/policy.html не найден</h2>", status_code=404)
    with open(path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())
      
@app.get("/policy/{token}", response_class=HTMLResponse)
def policy_page(token: str):
    # (обновление policy_viewed_at в БД у тебя уже есть)
    with open("static/policy.html", "r", encoding="utf-8") as f:
        html = f.read()
    return HTMLResponse(content=html)

WELCOME_IMAGE_PATH = "assets/welcome.png"

# =================== DB helpers ===================
def db():
    # Подключаемся на каждый запрос; psycopg сам держит соединение до закрытия контекста
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def init_db():
    with db() as con, con.cursor() as cur:
        # users
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            tg_id BIGINT PRIMARY KEY,
            email TEXT,
            phone TEXT,
            status TEXT DEFAULT 'new',      -- new|pending|legal_ok|active|expired
            policy_token TEXT,
            policy_viewed_at TIMESTAMPTZ,
            consent_viewed_at TIMESTAMPTZ,
            offer_viewed_at TIMESTAMPTZ,
            legal_confirmed_at TIMESTAMPTZ,
            valid_until TIMESTAMPTZ,
            last_invoice_id BIGINT,
            remind_3d_sent INT DEFAULT 0,
            created_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);")

        # payments
        cur.execute("""
        CREATE TABLE IF NOT EXISTS payments(
            inv_id BIGSERIAL PRIMARY KEY,
            tg_id BIGINT,
            out_sum NUMERIC(12,2),
            status TEXT,                 -- created|paid|failed
            created_at TIMESTAMPTZ,
            paid_at TIMESTAMPTZ,
            signature TEXT
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_tg ON payments(tg_id);")

        con.commit()

    with db() as con, con.cursor() as cur:
        cur.execute(ddl_users)
        cur.execute(ddl_payments)
        cur.execute(ddl_logs)

def now_ts():
    return datetime.now(timezone.utc)

def now_iso():
    return now_ts().isoformat()

def log_event(tg_id: int, event: str, data: str = None):
    try:
        with db() as con, con.cursor() as cur:
            cur.execute(
                "INSERT INTO logs(tg_id, event, data, created_at) VALUES(%s,%s,%s,%s)",
                (tg_id, event, data or "", now_ts())
            )
    except Exception:
        pass

def get_user(tg_id: int):
    with db() as con, con.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE tg_id=%s", (tg_id,))
        return cur.fetchone()

def upsert_user(tg_id: int, **kwargs):
    # не позволяем перезаписывать служебные поля извне
    reserved = {"tg_id", "created_at", "updated_at"}
    cleaned = {k: v for k, v in kwargs.items() if k not in reserved}

    row = get_user(tg_id)
    if row is None:
        # INSERT
        fields = list(cleaned.keys())
        cols = ["tg_id", "created_at", "updated_at"] + fields
        ph = ["%s"] * len(cols)
        vals = [tg_id, now_ts(), now_ts()] + list(cleaned.values())
        with db() as con, con.cursor() as cur:
            cur.execute(
                f"INSERT INTO users({','.join(cols)}) VALUES({','.join(ph)})",
                tuple(vals),
            )
    else:
        # UPDATE
        fields = list(cleaned.keys())
        if fields:
            set_parts = [f"{k}=%s" for k in fields] + ["updated_at=%s"]
            vals = list(cleaned.values()) + [now_ts(), tg_id]
            with db() as con, con.cursor() as cur:
                cur.execute(
                    f"UPDATE users SET {', '.join(set_parts)} WHERE tg_id=%s",
                    tuple(vals),
                )
        else:
            # даже если нечего обновлять — отметим updated_at
            with db() as con, con.cursor() as cur:
                cur.execute(
                    "UPDATE users SET updated_at=%s WHERE tg_id=%s",
                    (now_ts(), tg_id),
                )


def new_payment(tg_id: int, out_sum: float) -> int:
    with db() as con, con.cursor() as cur:
        cur.execute(
            "INSERT INTO payments(tg_id, out_sum, status, created_at) VALUES(%s,%s,%s,%s) RETURNING inv_id",
            (tg_id, out_sum, "created", now_ts())
        )
        inv_id = cur.fetchone()["inv_id"]
    upsert_user(tg_id, last_invoice_id=inv_id)
    log_event(tg_id, "payment_created", f"inv_id={inv_id}; sum={out_sum}")
    return inv_id

def set_payment_paid(inv_id: int):
    with db() as con, con.cursor() as cur:
        cur.execute("UPDATE payments SET status='paid', paid_at=%s WHERE inv_id=%s", (now_ts(), inv_id))

def set_payment_failed(inv_id: int):
    with db() as con, con.cursor() as cur:
        cur.execute("UPDATE payments SET status='failed' WHERE inv_id=%s", (inv_id,))

def list_active_users():
    with db() as con, con.cursor() as cur:
        cur.execute("SELECT tg_id, valid_until, remind_3d_sent FROM users WHERE status='active' AND valid_until IS NOT NULL")
        return cur.fetchall()

def is_admin(tg_id: int) -> bool:
    return ADMIN_USER_ID and tg_id == ADMIN_USER_ID

# =================== Robokassa ===================
def _sign(s: str) -> str:
    if ROBOKASSA_SIGNATURE_ALG == "SHA256":
        return sha256(s.encode('utf-8')).hexdigest()
    return md5(s.encode('utf-8')).hexdigest()

def sign_success(out_sum: float, inv_id: int) -> str:
    base = f"{ROBOKASSA_LOGIN}:{out_sum:.2f}:{inv_id}:{ROBOKASSA_PASSWORD1}"
    return _sign(base)

def sign_result(out_sum: float, inv_id: int) -> str:
    base = f"{out_sum:.2f}:{inv_id}:{ROBOKASSA_PASSWORD2}"
    return _sign(base)

def build_pay_url(inv_id: int, out_sum: float, description: str = "Подписка на 30 дней") -> str:
    out_sum_str = f"{out_sum:.2f}"
    base = f"{ROBOKASSA_LOGIN}:{out_sum_str}:{inv_id}:{ROBOKASSA_PASSWORD1}"
    signature = sha256(base.encode()).hexdigest() if ROBOKASSA_SIGNATURE_ALG == "SHA256" else md5(base.encode()).hexdigest()
    params = {
        "MerchantLogin": ROBOKASSA_LOGIN,
        "OutSum": out_sum_str,
        "InvId": str(inv_id),
        "Description": quote(description, safe=""),
        "SignatureValue": signature,
        "Culture": "ru",
        "Encoding": "utf-8",
    }
    if str(ROBOKASSA_TEST_MODE) == "0":
        params["IsTest"] = "0"
    return "https://auth.robokassa.ru/Merchant/Index.aspx?" + urlencode(params)

@dp.message(Command("pay"))
async def on_pay_cmd(message: Message):
    tg_id = message.from_user.id
    if not _legal_ok(tg_id):
        token = get_or_make_token(tg_id)
        await message.answer(
            "Сначала ознакомьтесь с документами и подтвердите согласие:",
            reply_markup=legal_keyboard(token)
        )
        return
    inv_id = new_payment(tg_id, PRICE_RUB)
    url = build_pay_url(inv_id, PRICE_RUB, "Подписка на 30 дней")
    await message.answer("Готово! Нажмите, чтобы оплатить:", reply_markup=pay_kb(url))

@dp.message(Command("stats"))
async def on_stats_cmd(message: Message):
    await on_stats(message)

@dp.message(Command("admin"))
async def on_admin_cmd(message: Message):
    await on_admin(message)

# =================== UI helpers ===================
  
def pay_kb(inv_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💳 Оплатить {int(PRICE_RUB)} 289₽ через Robokassa", url=inv_url)]
    ])

def renew_kb(inv_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🔁 Продлить за 289 {int(PRICE_RUB)} ₽", url=inv_url)]
    ])

def contact_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📲 Поделиться номером", request_contact=True)]],
        resize_keyboard=True, one_time_keyboard=True
    )

EMAIL_RE = re.compile(r"^[A-Za-z0-9_.+\-]+@[A-Za-z0-9\-]+\.[A-Za-z0-9\.\-]+$")

# =================== Handlers ===================
# Главное меню (кнопки снизу)
main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="💳 Оплатить подписку")],
        [KeyboardButton(text="📄 Документы")],
        [KeyboardButton(text="📊 Мой статус")],
    ],
    resize_keyboard=True,
    one_time_keyboard=False
)

def legal_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Политика конфиденциальности", url=f"{BASE_URL}/policy/{token}")],
        [InlineKeyboardButton(text="✅ Согласие на обработку данных", url=f"{BASE_URL}/consent/{token}")],
        [InlineKeyboardButton(text="📑 Публичная оферта", url=f"{BASE_URL}/offer/{token}")],
        [InlineKeyboardButton(text="✔️ Я ознакомился(лась)", callback_data=f"legal_agree:{token}")]
    ])

    return kb

def get_or_make_token(tg_id: int) -> str:
    u = get_user(tg_id)
    if u and u.get("policy_token"):
        return u["policy_token"]
    import secrets
    token = secrets.token_urlsafe(12)
    upsert_user(tg_id, policy_token=token, status="new")
    return token

@dp.message(F.text == "📄 Документы")
@dp.message(F.text == "/docs")
async def on_docs(message: Message):
    token = get_or_make_token(message.from_user.id)
    await message.answer("Документы:", reply_markup=legal_keyboard(token))

@dp.message(F.text == "💳 Оплатить подписку")
async def on_pay_btn(message: Message):
    inv_id = new_payment(message.from_user.id, PRICE_RUB)
    url = build_pay_url(inv_id, PRICE_RUB, "Подписка на 30 дней")
    await message.answer("Готово! Нажмите, чтобы оплатить:", reply_markup=pay_kb(url))

def _legal_ok(tg_id:int)->bool:
    with db() as con, con.cursor() as cur:
        cur.execute("SELECT legal_confirmed_at FROM users WHERE tg_id=%s", (tg_id,))
        r = cur.fetchone()
    return bool(r and r.get("legal_confirmed_at"))

@dp.message(F.text == "💳 Оплатить подписку")
async def on_pay_btn(message: Message):
    tg_id = message.from_user.id
    if not _legal_ok(tg_id):
        token = get_or_make_token(tg_id)
        await message.answer("Сначала ознакомьтесь с документами и подтвердите согласие:", reply_markup=legal_keyboard(token))
        return
    inv_id = new_payment(tg_id, PRICE_RUB)
    url = build_pay_url(inv_id, PRICE_RUB, "Подписка на 30 дней")
    print("[pay_link]", url)
    await message.answer("Готово! Нажмите, чтобы оплатить:", reply_markup=pay_kb(url))

@dp.message(F.text == "📊 Мой статус")
async def on_status_btn(message: Message):
    # Можно вызвать твою логику /stats или вставить её сюда
    await on_stats(message)  # если on_stats уже определён

@dp.message(CommandStart())
async def on_start(message: Message):
    tg_id = message.from_user.id
    token = get_or_make_token(tg_id)

    welcome_text = (
        "👋 Добро пожаловать!\n\n"
        "Перед использованием сервиса просим ознакомиться с документами.\n"
        "После подтверждения вы сможете ввести контакты и перейти к оплате."
    )

    # Если есть приветственная картинка — покажем её
    try:
        await message.answer_photo(
            FSInputFile("assets/welcome.png"),
            caption=welcome_text,
            reply_markup=legal_keyboard(token)
        )
    except Exception:
        await message.answer(welcome_text, reply_markup=legal_keyboard(token))

    # Показать главное меню (кнопки снизу)
    await message.answer("Выберите действие в меню ниже 👇", reply_markup=main_menu)

@dp.callback_query(F.data.startswith("legal_agree:"))
async def on_legal_agree(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]
    # Проверим, что все три документа были открыты (есть отметки)
    with db() as con, con.cursor() as cur:
        cur.execute("SELECT tg_id, policy_viewed_at, consent_viewed_at, offer_viewed_at FROM users WHERE policy_token=%s", (token,))
        row = cur.fetchone()
    if not row:
        await cb.answer("Сессия не найдена. Наберите /start", show_alert=True)
        return
    tg_id = row["tg_id"]
    if not (row.get("policy_viewed_at") and row.get("consent_viewed_at") and row.get("offer_viewed_at")):
        await cb.answer("Пожалуйста, сначала откройте все документы (Политика, Согласие, Оферта).", show_alert=True)
        return
    # Отмечаем согласие
    with db() as con, con.cursor() as cur:
        cur.execute("UPDATE users SET legal_confirmed_at=%s, status=%s WHERE tg_id=%s", (now_ts(), "legal_ok", tg_id))
    await cb.message.answer("Спасибо! ✅ Согласие зафиксировано.\nТеперь можно перейти к оплате.", reply_markup=pay_kb(build_pay_url(new_payment(tg_id, PRICE_RUB), PRICE_RUB, "Подписка на 30 дней")))
    await cb.answer()
  
@dp.message(F.contact)
async def on_contact(message: Message):
    phone = message.contact.phone_number
    upsert_user(message.from_user.id, phone=phone)
    await message.answer("Принял номер. Теперь введите ваш email (текстом).")
    log_event(message.from_user.id, "phone_saved", phone)

@dp.message(F.text)
async def on_email(message: Message):
    text = message.text.strip()
    user = get_user(message.from_user.id)
    if not user:
        return
    if user.get("policy_accepted_at") and not user.get("email"):
        if not EMAIL_RE.match(text):
            await message.answer("Похоже, это не email. Пример: name@example.com")
            return
        upsert_user(message.from_user.id, email=text)
        log_event(message.from_user.id, "email_saved", text)
        await message.answer("Готово! Формирую ссылку на оплату…")
        inv_id = new_payment(message.from_user.id, PRICE_RUB)
        url = build_pay_url(inv_id, PRICE_RUB, "Подписка на 30 дней")
        await message.answer(
            "Нажмите для оплаты. После успешной оплаты бот пришлёт приглашение в закрытый канал.",
            reply_markup=pay_kb(url)
        )

@dp.message(F.text == "/pay")
async def on_pay(message: Message):
    inv_id = new_payment(message.from_user.id, PRICE_RUB)
    url = build_pay_url(inv_id, PRICE_RUB, "Подписка на 30 дней")
    await message.answer("Готово! Нажмите, чтобы оплатить:", reply_markup=pay_kb(url))

def bar(progress: float, width: int = 20) -> str:
    filled = int(round(progress * width))
    return "▮" * filled + "▯" * (width - filled)

@dp.message(F.text == "/stats")
async def on_stats(message: Message):
    user = get_user(message.from_user.id)
    if not user or not user.get("valid_until"):
        await message.answer("Подписка пока не активна. Нажмите /start, оплатите — и вернёмся к статистике.")
        return

    vu = user["valid_until"]
    if isinstance(vu, str):
        try:
            vu = datetime.fromisoformat(vu)
        except Exception:
            await message.answer("Не удалось разобрать дату окончания. Нажмите /start.")
            return

    now = datetime.now(timezone.utc)
    total = timedelta(days=SUBSCRIPTION_DAYS)
    left = max(vu - now, timedelta(0))
    used = total - left
    progress = min(max(used / total, 0), 1)

    days_left = int(left.total_seconds() // 86400)
    hours_left = int((left.total_seconds() % 86400) // 3600)

    text = (
        f"📊 *Статус подписки*\n"
        f"До окончания: *{days_left} д. {hours_left} ч.*\n"
        f"`{bar(progress)}` {int(progress*100)}%\n"
        f"Действует до: `{vu.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}`"
    )

    if left <= timedelta(days=5):
        inv_id = new_payment(message.from_user.id, PRICE_RUB)
        url = build_pay_url(inv_id, PRICE_RUB, "Продление подписки на 30 дней")
        await message.answer(text, parse_mode="Markdown", reply_markup=renew_kb(url))
    else:
        await message.answer(text, parse_mode="Markdown")

@dp.message(Command("help"))
async def on_help(message: Message):
    await message.answer(
        "Команды:\n"
        "/start — начать\n"
        "/pay — получить ссылку на оплату\n"
        "/stats — статус подписки и прогресс-бар\n"
        "/admin — панель администратора (для владельца)\n"
        "/help — помощь"
    )

# =================== Admin mini panel ===================
@dp.message(F.text.startswith("/admin"))
async def on_admin(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Доступ запрещён.")
        return

    args = message.text.split()
    cmd = args[0]

    if cmd == "/admin" and len(args) == 1:
        await message.answer(
            "Админ-команды:\n"
            "/admin users — сводка пользователей\n"
            "/admin expiring [дней] — чьи подписки истекают в ближайшие N дней (по умолчанию 7)\n"
            "/admin revoke <tg_id> — снять доступ и пометить как expired\n"
            "/admin payments [N] — последние N платежей (по умолчанию 20)\n"
            "/admin logs [N] [event] — последние N логов, опционально по типу event\n"
            "/admin broadcast <текст> — рассылка всем активным"
        )
        return

    if len(args) >= 2 and args[1] == "users":
        with db() as con, con.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) c FROM users GROUP BY status")
            rows = cur.fetchall()
        lines = [f"{r['status'] or 'unknown'}: {r['c']}" for r in rows] or ["пусто"]
        await message.answer("Пользователи:\n" + "\n".join(lines))
        return

    if len(args) >= 2 and args[1] == "expiring":
        days = int(args[2]) if len(args) >= 3 and args[2].isdigit() else 7
        now = datetime.now(timezone.utc)
        lim = now + timedelta(days=days)
        with db() as con, con.cursor() as cur:
            cur.execute("SELECT tg_id, valid_until FROM users WHERE status='active' AND valid_until IS NOT NULL")
            rows = cur.fetchall()
        lst = []
        for r in rows:
            vu = r["valid_until"]
            if isinstance(vu, str):
                try:
                    vu = datetime.fromisoformat(vu)
                except Exception:
                    continue
            if now <= vu <= lim:
                lst.append(f"{r['tg_id']} — до {vu.strftime('%Y-%m-%d')}")
        await message.answer("Истекают в ближайшие {} дн.:\n".format(days) + ("\n".join(lst) if lst else "—"))
        return

    if len(args) >= 3 and args[1] == "revoke":
        try:
            target = int(args[2])
        except Exception:
            await message.answer("Укажи tg_id числом.")
            return
        try:
            await bot.ban_chat_member(CHANNEL_ID, target)
            await bot.unban_chat_member(CHANNEL_ID, target)
        except Exception:
            pass
        upsert_user(target, status="expired", remind_3d_sent=0)
        log_event(message.from_user.id, "admin_revoke", f"target={target}")
        await message.answer(f"Доступ снят: {target}")
        return

    if len(args) >= 2 and args[1] == "payments":
        n = int(args[2]) if len(args) >= 3 and args[2].isdigit() else 20
        with db() as con, con.cursor() as cur:
            cur.execute("SELECT inv_id, tg_id, out_sum, status, created_at, paid_at FROM payments ORDER BY inv_id DESC LIMIT %s", (n,))
            rows = cur.fetchall()
        lines = [f"#{r['inv_id']} tg={r['tg_id']} sum={r['out_sum']} status={r['status']} created={r['created_at']} paid={r['paid_at'] or '-'}" for r in rows] or ["—"]
        await message.answer("Платежи:\n" + "\n".join(lines))
        return

    if len(args) >= 2 and args[1] == "logs":
        n = 50
        event_filter = None
        if len(args) >= 3 and args[2].isdigit():
            n = int(args[2])
            if len(args) >= 4:
                event_filter = args[3]
        elif len(args) >= 3:
            event_filter = args[2]

        with db() as con, con.cursor() as cur:
            if event_filter:
                cur.execute("SELECT id,tg_id,event,data,created_at FROM logs WHERE event=%s ORDER BY id DESC LIMIT %s", (event_filter, n))
            else:
                cur.execute("SELECT id,tg_id,event,data,created_at FROM logs ORDER BY id DESC LIMIT %s", (n,))
            rows = cur.fetchall()
        lines = [f"{r['id']}: {r['created_at']} tg={r['tg_id']} {r['event']} {('- ' + r['data']) if r['data'] else ''}" for r in rows] or ["—"]
        await message.answer("Логи:\n" + "\n".join(lines))
        return

    if len(args) >= 3 and args[1] == "broadcast":
        text = message.text.split(" ", 2)[2].strip()
        with db() as con, con.cursor() as cur:
            cur.execute("SELECT tg_id FROM users WHERE status='active'")
            rows = cur.fetchall()
        sent = 0
        for r in rows:
            try:
                await bot.send_message(r["tg_id"], text)
                sent += 1
            except Exception:
                pass
        log_event(message.from_user.id, "admin_broadcast", f"sent={sent}")
        await message.answer(f"Отправлено: {sent}")
        return

    await message.answer("Неизвестная команда. Введите /admin.")

# =================== Webhook ===================
@app.post(f"/telegram/webhook/{WEBHOOK_SECRET}")
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}

async def set_webhook():
    await bot.set_webhook(f"{BASE_URL}/telegram/webhook/{WEBHOOK_SECRET}")

# =================== Policy pages ===================
@app.get("/policy/{token}", response_class=HTMLResponse)
def policy_page(token: str):
    with db() as con, con.cursor() as cur:
        cur.execute("UPDATE users SET policy_viewed_at=%s WHERE policy_token=%s", (now_ts(), token))
    with open("static/policy.html", "r", encoding="utf-8") as f:
        html = f.read()
    return HTMLResponse(content=html)

def _read_html(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

@app.get("/policy/{token}", response_class=HTMLResponse)
def policy_with_token(token: str):
    try:
        with db() as con, con.cursor() as cur:
            cur.execute("UPDATE users SET policy_viewed_at=%s WHERE policy_token=%s", (now_ts(), token))
    except Exception as e:
        print("policy update failed:", e)
    return HTMLResponse(_read_html("static/policy.html"))

@app.get("/consent/{token}", response_class=HTMLResponse)
def consent_with_token(token: str):
    print(f"[DOC] open consent token={token}")
    try:
        with db() as con, con.cursor() as cur:
            cur.execute("UPDATE users SET consent_viewed_at=%s WHERE policy_token=%s", (now_ts(), token))
    except Exception as e:
        print("consent update failed:", e)
    return HTMLResponse(_read_html("static/consent.html"))

@app.get("/offer/{token}", response_class=HTMLResponse)
def offer_with_token(token: str):
    print(f"[DOC] open offer token={token}")
    try:
        with db() as con, con.cursor() as cur:
            cur.execute("UPDATE users SET offer_viewed_at=%s WHERE policy_token=%s", (now_ts(), token))
    except Exception as e:
        print("offer update failed:", e)
    return HTMLResponse(_read_html("static/offer.html"))

# опционально «плоские» проверки статики
@app.get("/policy", response_class=HTMLResponse)
def policy_plain():  return HTMLResponse(_read_html("static/policy.html"))

@app.get("/consent", response_class=HTMLResponse)
def consent_plain(): return HTMLResponse(_read_html("static/consent.html"))

@app.get("/offer", response_class=HTMLResponse)
def offer_plain():   return HTMLResponse(_read_html("static/offer.html"))
# =================== Robokassa callbacks ===================
class RobokassaResult(BaseModel):
    OutSum: float
    InvId: int
    SignatureValue: str

def _equals_ci(a: str, b: str) -> bool:
    return a.lower() == b.lower()

@app.post("/pay/result")
async def pay_result(form: Request):
    data = dict((await form.form()))
    try:
        out_sum = float(data.get("OutSum"))
        inv_id = int(data.get("InvId"))
        sig = data.get("SignatureValue")
    except Exception:
        raise HTTPException(400, "Bad payload")

    expected = sign_result(out_sum, inv_id)
    if not _equals_ci(sig, expected):
        set_payment_failed(inv_id)
        log_event(0, "payment_bad_sig", f"inv_id={inv_id}")
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
    log_event(tg_id, "payment_paid", f"inv_id={inv_id}; sum={out_sum}")

    try:
        expire_at = now_ts() + timedelta(days=2)
        link = await bot.create_chat_invite_link(
            chat_id=CHANNEL_ID,
            name=f"Sub {tg_id} {inv_id}",
            expire_date=int(expire_at.timestamp()),
            member_limit=1
        )
        await bot.send_message(tg_id, f"Оплата получена ✅\n\nВаша ссылка в закрытый канал:\n{link.invite_link}\n\n(действует ограниченно и одноразово)")
        log_event(tg_id, "invite_sent")
    except Exception as e:
        if ADMIN_USER_ID:
            await bot.send_message(ADMIN_USER_ID, f"Не удалось создать инвайт: {e}")
        log_event(tg_id, "invite_failed", str(e))

    return PlainTextResponse(f"OK{inv_id}")

@app.get("/pay/success")
def pay_success():
    return HTMLResponse("<h2>Спасибо! Оплата прошла. Вернитесь в Telegram — приглашение уже ждёт вас в боте.</h2>")

@app.get("/pay/fail")
def pay_fail():
    return HTMLResponse("<h2>Оплата не завершена. Вы можете повторить попытку в боте.</h2>")

# =================== Cron: reminders & expiry ===================
async def check_expired():
    # 1) Напоминания за 3 дня
    now = now_ts()
    rows = list_active_users()
    for r in rows:
        vu = r["valid_until"]
        if isinstance(vu, str):
            try:
                vu = datetime.fromisoformat(vu)
            except Exception:
                continue
        remind_sent = int(r.get("remind_3d_sent") or 0) == 1
        if (not remind_sent) and (now + timedelta(days=3) >= vu > now):
            try:
                inv = new_payment(r["tg_id"], PRICE_RUB)
                url = build_pay_url(inv, PRICE_RUB, "Продление подписки на 30 дней")
                await bot.send_message(
                    r["tg_id"],
                    "⏰ Напоминание: через *3 дня* истекает подписка. Продлите, чтобы не потерять доступ:",
                    parse_mode="Markdown",
                    reply_markup=renew_kb(url)
                )
                upsert_user(r["tg_id"], remind_3d_sent=1)
                log_event(r["tg_id"], "reminder_3d_sent")
            except Exception as e:
                log_event(r["tg_id"], "reminder_3d_failed", str(e))

    # 2) Снятие доступа по истечении
    rows = list_active_users()
    for r in rows:
        vu = r["valid_until"]
        if isinstance(vu, str):
            try:
                vu = datetime.fromisoformat(vu)
            except Exception:
                continue
        if now >= vu:
            tg_id = r["tg_id"]
            try:
                await bot.ban_chat_member(CHANNEL_ID, tg_id)
                await bot.unban_chat_member(CHANNEL_ID, tg_id)
            except Exception as e:
                log_event(tg_id, "revoke_fail", str(e))
            upsert_user(tg_id, status="expired", remind_3d_sent=0)
            try:
                inv = new_payment(tg_id, PRICE_RUB)
                url = build_pay_url(inv, PRICE_RUB, "Продление подписки на 30 дней")
                await bot.send_message(
                    tg_id,
                    "⛔ Подписка истекла. Чтобы вернуться в закрытый канал — продлите:",
                    reply_markup=renew_kb(url)
                )
            except Exception:
                pass
            log_event(tg_id, "access_revoked")

@app.get("/cron/ping")
async def cron_ping():
    asyncio.create_task(check_expired())
    return {"ok": True}

# =================== Startup ===================
@app.on_event("startup")
async def startup():
    os.makedirs("static", exist_ok=True)
    os.makedirs("assets", exist_ok=True)

    # локальная функция для автосоздания html-файлов
    def ensure(path: str, content: str):
        if not os.path.exists(path):
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            
    ensure(
        "static/policy.html",
        """<!doctype html><html lang="ru"><meta charset="utf-8">
<title>Политика конфиденциальности</title>
<style>body{font:16px/1.6 system-ui, sans-serif;max-width:840px;margin:40px auto;padding:0 16px}</style>
<h1>Политика конфиденциальности</h1>
<p>Факт открытия страницы фиксируется для подтверждения ознакомления.</p>
</html>"""
    )
    ensure(
        "static/consent.html",
        """<!doctype html><html lang="ru"><meta charset="utf-8">
<title>Согласие на обработку персональных данных</title>
<style>body{font:16px/1.6 system-ui, sans-serif;max-width:840px;margin:40px auto;padding:0 16px}</style>
<h1>Согласие на обработку персональных данных</h1>
<p>Нажимая кнопки в боте, вы даёте согласие на обработку ПДн в целях оказания услуги.</p>
</html>"""
    )
    ensure(
        "static/offer.html",
        """<!doctype html><html lang="ru"><meta charset="utf-8">
<title>Договор публичной оферты</title>
<style>body{font:16px/1.6 system-ui, sans-serif;max-width:840px;margin:40px auto;padding:0 16px}</style>
<h1>Договор публичной оферты</h1>
<p>Оплата подписки означает акцепт условий настоящей оферты.</p>
</html>"""
    )
    
    init_db()
    await set_webhook()

    async def loop():
        while True:
            await check_expired()
            await asyncio.sleep(3600)
    asyncio.create_task(loop())
