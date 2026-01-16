import asyncio
import logging
import os
import sys
import asyncpg
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import CallbackQuery
from dotenv import load_dotenv
from openai import AsyncOpenAI

# --- КОНФІГУРАЦІЯ ---
load_dotenv()

API_TOKEN = os.getenv("BOT_TOKEN")
NEON_URL = os.getenv("NEON_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

ADMIN_ID = 548789253
TARGET_CHAT_ID = -1001981383150

# Глибина контексту
THREAD_DEPTH_LIMIT = 10 

if not API_TOKEN or not NEON_URL:
    print("❌ ПОМИЛКА: Перевір .env файл")
    sys.exit(1)

logging.basicConfig(level=logging.INFO)

bot = Bot(token=API_TOKEN)
dp = Dispatcher()
db_pool = None

gpt_client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# Налаштування за замовчуванням
DEFAULT_SYSTEM_PROMPT = None 
DEFAULT_TEMPERATURE = 1.0  
DEFAULT_MODEL = "gpt-5-mini" 

AVAILABLE_MODELS = [
    "gpt-5-mini",
    "gpt-5.2-chat-latest",
    "gpt-5-pro"
]

# --- БАЗА ДАНИХ ---
async def create_pool():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(dsn=NEON_URL)
        logging.info("✅ База підключена")
    except Exception as e:
        logging.error(f"❌ Не вдалося підключитися до бази: {e}")
        sys.exit(1)

# --- ФУНКЦІЯ ЗАПИСУ ---
async def save_to_db(message: types.Message):
    user = message.from_user
    chat = message.chat
    if not user: return

    async with db_pool.acquire() as con:
        await con.execute("""
            INSERT INTO users (user_id, username, first_name, last_name)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id) DO UPDATE 
            SET username = EXCLUDED.username, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name
        """, user.id, user.username, user.first_name, user.last_name)

        await con.execute("""
            INSERT INTO chats (chat_id, type, title)
            VALUES ($1, $2, $3)
            ON CONFLICT (chat_id) DO UPDATE SET type = EXCLUDED.type, title = EXCLUDED.title
        """, chat.id, chat.type, chat.title)

        msg_date = message.date.replace(tzinfo=None)
        reply_to = message.reply_to_message.message_id if message.reply_to_message else None
        
        msg_type = 'text'
        if message.photo: msg_type = 'photo'
        elif message.sticker: msg_type = 'sticker'

        await con.execute("""
            INSERT INTO msg_meta (chat_id, msg_id, user_id, date_msg, msg_type, reply_to)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (chat_id, msg_id) DO NOTHING
        """, chat.id, message.message_id, user.id, msg_date, msg_type, reply_to)

        if message.text:
            await con.execute("""
                INSERT INTO msg_txt (chat_id, msg_id, msg_txt)
                VALUES ($1, $2, $3)
                ON CONFLICT (chat_id, msg_id) DO NOTHING
            """, chat.id, message.message_id, message.text)
        elif message.photo:
            photo = message.photo[-1]
            await con.execute("""
                INSERT INTO photo (chat_id, msg_id, photo_url, caption)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (chat_id, msg_id) DO NOTHING
            """, chat.id, message.message_id, photo.file_id, message.caption)

# --- ЛОГІКА ІСТОРІЇ (THREAD) ---
async def get_thread_context(chat_id, start_msg_id):
    async with db_pool.acquire() as con:
        sql = """
            WITH RECURSIVE thread AS (
                SELECT m.msg_id, m.reply_to, m.user_id, m.date_msg, t.msg_txt, 1 as depth
                FROM msg_meta m
                JOIN msg_txt t ON m.chat_id = t.chat_id AND m.msg_id = t.msg_id
                WHERE m.chat_id = $1 AND m.msg_id = $2
                UNION ALL
                SELECT parent.msg_id, parent.reply_to, parent.user_id, parent.date_msg, pt.msg_txt, thread.depth + 1
                FROM msg_meta parent
                JOIN msg_txt pt ON parent.chat_id = pt.chat_id AND parent.msg_id = pt.msg_id
                JOIN thread ON thread.reply_to = parent.msg_id
                WHERE parent.chat_id = $1 AND thread.depth < $3
            )
            SELECT thread.*, u.first_name 
            FROM thread
            LEFT JOIN users u ON thread.user_id = u.user_id
            ORDER BY thread.date_msg ASC;
        """
        rows = await con.fetch(sql, chat_id, start_msg_id, THREAD_DEPTH_LIMIT)
        return rows

# --- 🔥 ХЕЛПЕР: ПЕРЕВІРКА НА "УЗБЕКІВ" ---
async def check_for_sleeping_uzbeks(message: types.Message):
    """
    Перевіряє, чи є в повідомленні теги людей з ignorehere.
    Якщо є - відправляє попередження.
    """
    if not message.entities: return

    mentioned_ids = []
    mentioned_usernames = []

    for entity in message.entities:
        if entity.type == 'text_mention':
            mentioned_ids.append(entity.user.id)
        elif entity.type == 'mention':
            # Витягуємо юзернейм без @
            uname = message.text[entity.offset + 1 : entity.offset + entity.length]
            mentioned_usernames.append(uname)

    if not mentioned_ids and not mentioned_usernames:
        return

    chat_id = message.chat.id
    try:
        async with db_pool.acquire() as con:
            sql = """
                SELECT 1 
                FROM here_ignore hi
                LEFT JOIN users u ON hi.user_id = u.user_id
                WHERE hi.chat_id = $1 
                  AND (hi.user_id = ANY($2) OR u.username = ANY($3))
                LIMIT 1
            """
            exists = await con.fetchval(sql, chat_id, mentioned_ids, mentioned_usernames)
            
            if exists:
                await message.reply("ЧШШШШ УЗБЕКІ СПЯТЬ")
    except Exception as e:
        logging.error(f"Mention Check Error: {e}")


# --- ХЕЛПЕР ВІДПРАВКИ (БРОНЕБІЙНИЙ) ---
async def send_chunked_response(message_obj, text):
    async def send_safe(chunk):
        try:
            return await message_obj.answer(chunk, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            try:
                return await message_obj.answer(chunk, parse_mode=None)
            except Exception as e:
                logging.error(f"Failed to send chunk: {e}")
                return None

    if len(text) <= 4000:
        sent = await send_safe(text)
        if sent: await save_to_db(sent)
    else:
        chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for chunk in chunks:
            sent = await send_safe(chunk)
            if sent: await save_to_db(sent)

def get_cutoff_date(period_code):
    now = datetime.utcnow()
    if period_code == '1d': return now - timedelta(days=1)
    elif period_code == '7d': return now - timedelta(days=7)
    elif period_code == '30d': return now - timedelta(days=30)
    return None

def get_period_name(period_code):
    if period_code == '1d': return "24 години"
    if period_code == '7d': return "7 днів"
    if period_code == '30d': return "30 днів"
    return "Весь час"


# ==========================================
# ХЕНДЛЕРИ НАЛАШТУВАНЬ
# ==========================================

@dp.message(F.text.startswith('!system'))
async def cmd_set_system(message: types.Message):
    await save_to_db(message)
    new_prompt = message.text[8:].strip()
    chat_id = message.chat.id
    if not new_prompt:
        async with db_pool.acquire() as con:
            current = await con.fetchval("SELECT system_prompt FROM chats WHERE chat_id=$1", chat_id)
        display = current if current else "(Пусто / Стандарт)"
        await message.answer(f"🧠 <b>Поточна установка:</b>\n<code>{display}</code>", parse_mode=ParseMode.HTML)
        return
    try:
        async with db_pool.acquire() as con:
            await con.execute("UPDATE chats SET system_prompt=$1 WHERE chat_id=$2", new_prompt, chat_id)
        await message.answer(f"✅ <b>Нова особистість:</b> {new_prompt}", parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.answer(f"Помилка БД: {e}")

@dp.message(F.text.lower().startswith('!clearsystem'))
async def cmd_clear_system(message: types.Message):
    await save_to_db(message)
    chat_id = message.chat.id
    try:
        async with db_pool.acquire() as con:
            await con.execute("UPDATE chats SET system_prompt=NULL WHERE chat_id=$1", chat_id)
        await message.answer("🔄 <b>Системну установку видалено.</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.answer(f"Помилка: {e}")

@dp.message(F.text.lower().startswith('!forget'))
async def cmd_forget(message: types.Message):
    await save_to_db(message)
    await message.answer("🧹 <b>Контекст 'освіжено'.</b> (Візуально). Справжній контекст залежить від reply-ланцюжка.", parse_mode=ParseMode.HTML)

@dp.message(F.text.lower().startswith('!temp'))
async def cmd_set_temp(message: types.Message):
    await save_to_db(message)
    args = message.text.split()
    chat_id = message.chat.id
    if len(args) < 2:
        async with db_pool.acquire() as con:
            current = await con.fetchval("SELECT temperature FROM chats WHERE chat_id=$1", chat_id)
        val = current if current is not None else DEFAULT_TEMPERATURE
        await message.answer(f"🌡 <b>Поточна температура:</b> {val}", parse_mode=ParseMode.HTML)
        return
    try:
        new_temp = float(args[1])
        if not (0.0 <= new_temp <= 2.0):
            await message.answer("❌ Температура має бути від 0.0 до 2.0")
            return
        async with db_pool.acquire() as con:
            await con.execute("UPDATE chats SET temperature=$1 WHERE chat_id=$2", new_temp, chat_id)
        await message.answer(f"🌡 <b>Встановлено:</b> {new_temp}", parse_mode=ParseMode.HTML)
    except ValueError:
        await message.answer("❌ Введи число (наприклад 1.0).")

# !models
@dp.message(F.text.lower().startswith('!models') | F.text.lower().startswith('!model'))
async def cmd_models_menu(message: types.Message):
    await save_to_db(message)
    chat_id = message.chat.id
    async with db_pool.acquire() as con:
        current_model = await con.fetchval("SELECT model_name FROM chats WHERE chat_id=$1", chat_id)
    current_model = current_model if current_model else DEFAULT_MODEL
    text = f"💾 <b>Поточна модель:</b> <code>{current_model}</code>\n\n👇 <b>Обери нову:</b>"
    builder = InlineKeyboardBuilder()
    for model in AVAILABLE_MODELS:
        label = f"✅ {model}" if model == current_model else model
        builder.button(text=label, callback_data=f"set_mdl_{model}")
    builder.adjust(1)
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("set_mdl_"))
async def cb_set_model(callback: CallbackQuery):
    selected_model = callback.data.replace("set_mdl_", "")
    chat_id = callback.message.chat.id
    if selected_model not in AVAILABLE_MODELS:
        await callback.answer("❌ Невідома модель.", show_alert=True); return
    try:
        async with db_pool.acquire() as con:
            await con.execute("UPDATE chats SET model_name=$1 WHERE chat_id=$2", selected_model, chat_id)
        await callback.message.edit_text(f"💾 <b>Встановлено модель:</b> <code>{selected_model}</code>", parse_mode=ParseMode.HTML)
    except Exception as e: await callback.message.edit_text(f"Помилка БД: {e}")


# ==========================================
# ХЕНДЛЕРИ АНАЛІЗУ (!analyze)
# ==========================================
@dp.message(F.text.lower().startswith('!analyze'))
async def cmd_analyze_menu(message: types.Message):
    await save_to_db(message)
    chat_id = message.chat.id
    sql = "SELECT u.user_id, u.first_name, COUNT(m.msg_id) as cnt FROM msg_meta m JOIN users u ON m.user_id = u.user_id JOIN msg_txt t ON m.chat_id = t.chat_id AND m.msg_id = t.msg_id WHERE m.chat_id = $1 GROUP BY u.user_id, u.first_name ORDER BY cnt DESC LIMIT 20"
    try:
        async with db_pool.acquire() as con: rows = await con.fetch(sql, chat_id)
    except: await message.answer("Помилка БД."); return
    if not rows: await message.answer("Немає текстових даних."); return
    builder = InlineKeyboardBuilder()
    for row in rows: builder.button(text=row['first_name'], callback_data=f"anlz_sel_{row['user_id']}")
    builder.adjust(2)
    await message.answer("🕵️‍♂️ <b>Кого аналізуємо?</b>", parse_mode=ParseMode.HTML, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("anlz_sel_"))
async def cb_analyze_select_count(callback: CallbackQuery):
    target_uid = callback.data.split("_")[2]
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 25", callback_data=f"anlz_run_{target_uid}_25")
    builder.button(text="📝 50", callback_data=f"anlz_run_{target_uid}_50")
    builder.button(text="📝 100", callback_data=f"anlz_run_{target_uid}_100")
    builder.button(text="📝 1000 (VIP/24h)", callback_data=f"anlz_run_{target_uid}_1000")
    builder.adjust(2)
    await callback.message.edit_text("🔢 <b>Скільки повідомлень?</b>", parse_mode=ParseMode.HTML, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("anlz_run_"))
async def cb_analyze_run(callback: CallbackQuery):
    parts = callback.data.split("_")
    target_uid, limit, chat_id = int(parts[2]), int(parts[3]), callback.message.chat.id
    caller_id = callback.from_user.id  
    
    if not gpt_client: await callback.message.edit_text("❌ AI не підключено."); return

    if limit == 1000 and caller_id != ADMIN_ID:
        try:
            async with db_pool.acquire() as con:
                last_run = await con.fetchval("SELECT last_1000_analyze FROM users WHERE user_id=$1", caller_id)
                if last_run:
                    next_run = last_run + timedelta(hours=24)
                    if datetime.utcnow() < next_run:
                        diff = next_run - datetime.utcnow()
                        h, r = divmod(diff.seconds, 3600); m, _ = divmod(r, 60)
                        await callback.answer(f"⛔ Ліміт! Чекай: {h}год {m}хв", show_alert=True); return
                await con.execute("UPDATE users SET last_1000_analyze=$1 WHERE user_id=$2", datetime.utcnow(), caller_id)
        except: pass

    await callback.message.edit_text("⏳ <b>Збираю архів...</b>", parse_mode=ParseMode.HTML)
    
    sql = "SELECT t.msg_txt FROM msg_meta m JOIN msg_txt t ON m.chat_id = t.chat_id AND m.msg_id = t.msg_id WHERE m.chat_id = $1 AND m.user_id = $2 AND t.msg_txt IS NOT NULL AND t.msg_txt != '' ORDER BY m.date_msg DESC LIMIT $3"
    try:
        async with db_pool.acquire() as con:
            rows = await con.fetch(sql, chat_id, target_uid, limit)
            user_name = await con.fetchval("SELECT first_name FROM users WHERE user_id=$1", target_uid)
            chat_model = await con.fetchval("SELECT model_name FROM chats WHERE chat_id=$1", chat_id)
        current_model = chat_model if chat_model else DEFAULT_MODEL
        texts = [r['msg_txt'] for r in rows if r['msg_txt'] and str(r['msg_txt']).strip()]
        if not texts: await callback.message.edit_text("❌ Тільки картинки."); return
        text_dump = "\n".join(texts)
    except: await callback.message.edit_text("Помилка БД."); return

    sys_instr = "Ти — досвідчений психоаналітик."
    prompt = f"Проаналізуй: {user_name}. Текст:\n{text_dump}"
    
    try:
        response = await gpt_client.chat.completions.create(model=current_model, messages=[{"role":"system","content":sys_instr},{"role":"user","content":prompt}])
        await callback.message.delete()
        await send_chunked_response(callback.message, f"🧠 <b>Аналіз {user_name}:</b>\n\n{response.choices[0].message.content}")
    except Exception as e:
        await callback.message.answer(f"⚠️ Помилка AI: {e}")


# ==========================================
# СТАТИСТИКА І Т.Д.
# ==========================================
@dp.message(F.text.lower().startswith('!stats'))
async def cmd_stats_menu(message: types.Message):
    await save_to_db(message)
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Вся група", callback_data="ask_period_group")
    builder.button(text="👤 Конкретний юзер", callback_data="ask_period_user")
    builder.adjust(1)
    await message.answer("Чию статистику дивимось?", reply_markup=builder.as_markup())

@dp.callback_query(F.data == "stats_main_menu")
async def cb_back_to_main(callback: CallbackQuery):
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Вся група", callback_data="ask_period_group")
    builder.button(text="👤 Конкретний юзер", callback_data="ask_period_user")
    builder.adjust(1)
    await callback.message.edit_text("Чию статистику дивимось?", reply_markup=builder.as_markup())
    await callback.answer()

async def send_period_menu(message, prefix):
    builder = InlineKeyboardBuilder()
    builder.button(text="📅 1 День", callback_data=f"{prefix}1d")
    builder.button(text="📅 1 Тиждень", callback_data=f"{prefix}7d")
    builder.button(text="📅 1 Місяць", callback_data=f"{prefix}30d")
    builder.button(text="📅 Весь час", callback_data=f"{prefix}all")
    builder.button(text="🔙 Назад", callback_data="stats_main_menu")
    builder.adjust(2)
    await message.edit_text("За який період?", reply_markup=builder.as_markup())

@dp.callback_query(F.data == "ask_period_group")
async def cb_ask_period_group(callback: CallbackQuery):
    await send_period_menu(callback.message, "res_grp_")
    await callback.answer()

@dp.callback_query(F.data == "ask_period_user")
async def cb_ask_period_user(callback: CallbackQuery):
    await send_period_menu(callback.message, "list_usr_")
    await callback.answer()

@dp.callback_query(F.data.startswith("res_grp_"))
async def cb_show_group_stats(callback: CallbackQuery):
    period = callback.data.split("_")[2]
    chat_id = callback.message.chat.id
    cutoff = get_cutoff_date(period)
    async with db_pool.acquire() as con:
        if cutoff:
            total_msgs = await con.fetchval("SELECT COUNT(*) FROM msg_meta WHERE chat_id=$1 AND date_msg >= $2", chat_id, cutoff)
            top_rows = await con.fetch("SELECT u.first_name, COUNT(m.msg_id) as cnt FROM msg_meta m JOIN users u ON m.user_id = u.user_id WHERE m.chat_id = $1 AND m.date_msg >= $2 GROUP BY u.first_name ORDER BY cnt DESC LIMIT 5", chat_id, cutoff)
        else:
            total_msgs = await con.fetchval("SELECT COUNT(*) FROM msg_meta WHERE chat_id=$1", chat_id)
            top_rows = await con.fetch("SELECT u.first_name, COUNT(m.msg_id) as cnt FROM msg_meta m JOIN users u ON m.user_id = u.user_id WHERE m.chat_id = $1 GROUP BY u.first_name ORDER BY cnt DESC LIMIT 5", chat_id)
    text = f"📊 <b>Статистика ({get_period_name(period)})</b>\n\n💬 Всього: <b>{total_msgs}</b>\n\n"
    if total_msgs > 0:
        text += "🏆 <b>ТОП-5:</b>\n"
        for i, row in enumerate(top_rows, 1): text += f"{i}. {str(row['first_name']).replace('<','&lt;')} — {row['cnt']}\n"
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 Назад", callback_data="ask_period_group")
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("list_usr_"))
async def cb_show_user_list(callback: CallbackQuery):
    period = callback.data.split("_")[2]
    chat_id = callback.message.chat.id
    cutoff = get_cutoff_date(period)
    async with db_pool.acquire() as con:
        if cutoff: rows = await con.fetch("SELECT u.user_id, u.first_name, COUNT(m.msg_id) as cnt FROM msg_meta m JOIN users u ON m.user_id = u.user_id WHERE m.chat_id = $1 AND m.date_msg >= $2 GROUP BY u.user_id, u.first_name ORDER BY cnt DESC LIMIT 20", chat_id, cutoff)
        else: rows = await con.fetch("SELECT u.user_id, u.first_name, COUNT(m.msg_id) as cnt FROM msg_meta m JOIN users u ON m.user_id = u.user_id WHERE m.chat_id = $1 GROUP BY u.user_id, u.first_name ORDER BY cnt DESC LIMIT 20", chat_id)
    if not rows: await callback.answer("Пусто.", show_alert=True); return
    builder = InlineKeyboardBuilder()
    for row in rows: builder.button(text=row['first_name'], callback_data=f"stat_u_{row['user_id']}_{period}")
    builder.adjust(2)
    builder.button(text="🔙 Назад", callback_data="ask_period_user")
    await callback.message.edit_text(f"Топ-20 за {get_period_name(period)}:", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("stat_u_"))
async def cb_user_details(callback: CallbackQuery):
    parts = callback.data.split("_")
    uid, period = int(parts[2]), parts[3]
    chat_id, cutoff = callback.message.chat.id, get_cutoff_date(period)
    async with db_pool.acquire() as con:
        name = await con.fetchval("SELECT first_name FROM users WHERE user_id=$1", uid)
        if cutoff:
            total = await con.fetchval("SELECT COUNT(*) FROM msg_meta WHERE chat_id=$1 AND user_id=$2 AND date_msg >= $3", chat_id, uid, cutoff)
            stats = await con.fetch("SELECT msg_type, COUNT(*) as cnt FROM msg_meta WHERE chat_id=$1 AND user_id=$2 AND date_msg >= $3 GROUP BY msg_type ORDER BY cnt DESC", chat_id, uid, cutoff)
        else:
            total = await con.fetchval("SELECT COUNT(*) FROM msg_meta WHERE chat_id=$1 AND user_id=$2", chat_id, uid)
            stats = await con.fetch("SELECT msg_type, COUNT(*) as cnt FROM msg_meta WHERE chat_id=$1 AND user_id=$2 GROUP BY msg_type ORDER BY cnt DESC", chat_id, uid)
    text = f"👤 <b>{str(name).replace('<','&lt;')}</b> ({get_period_name(period)})\n📨: <b>{total}</b>\n"
    for r in stats: text += f"🔹 {r['msg_type']}: {r['cnt']}\n"
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 До списку", callback_data=f"list_usr_{period}")
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=builder.as_markup())
    await callback.answer()

@dp.message(F.text.lower().startswith('!ignorehere'))
async def cmd_ignore_here(message: types.Message):
    await save_to_db(message)
    user_id, chat_id = message.from_user.id, message.chat.id
    async with db_pool.acquire() as con:
        res = await con.execute("DELETE FROM here_ignore WHERE chat_id=$1 AND user_id=$2", chat_id, user_id)
        if res == "DELETE 1": await message.answer("👻 <b>Ти знову в грі!</b>", parse_mode=ParseMode.HTML)
        else:
            await con.execute("INSERT INTO here_ignore (chat_id, user_id) VALUES ($1, $2)", chat_id, user_id)
            await message.answer("🔕 <b>Режим невидимки ввімкнено.</b>", parse_mode=ParseMode.HTML)

@dp.message(F.text.lower().startswith('!help'))
async def cmd_help(message: types.Message):
    await save_to_db(message)
    text = ("🤖 <b>Команди:</b>\n💬 <b>!текст</b> — GPT\n💾 <b>!models</b> — Вибір моделі\n🕵️‍♂️ <b>!analyze</b> — Психоаналіз\n"
            "🧠 <b>!system</b> — Особистість\n🔄 <b>!clearsystem</b> — Скидання\n🧹 <b>!forget</b> — Очистка контексту\n"
            "🌡 <b>!temp</b> — Креативність\n📊 <b>!stats</b> — Статистика\n📢 <b>!here</b> — Всіх тегнути\n"
            "🔕 <b>!ignorehere</b> — Сховатися від !here\n🎲 <b>!roulette</b> — Рулетка")
    await message.answer(text, parse_mode=ParseMode.HTML)

@dp.message(F.text.lower().startswith('!roulette'))
async def cmd_roulette(message: types.Message):
    if message.chat.type == 'private': return 
    await save_to_db(message)
    chat_id = message.chat.id
    sql = """
        SELECT * FROM (
            SELECT DISTINCT u.user_id, u.username, u.first_name
            FROM users u
            JOIN msg_meta m ON u.user_id = m.user_id
            LEFT JOIN here_ignore hi ON u.user_id = hi.user_id AND m.chat_id = hi.chat_id
            WHERE m.chat_id = $1 AND hi.user_id IS NULL
        ) t ORDER BY RANDOM() LIMIT 1
    """
    try:
        async with db_pool.acquire() as con: row = await con.fetchrow(sql, chat_id)
        if not row: return
        m = f"@{row['username']}" if row['username'] else f"<a href='tg://user?id={row['user_id']}'>{row['first_name']}</a>"
        await message.answer(f"{m} - НУ ТИ І ПІДАРАС", parse_mode=ParseMode.HTML)
    except: pass

@dp.message(F.text.lower().startswith('!here'))
async def cmd_here(message: types.Message):
    await save_to_db(message)
    chat_id = message.chat.id
    sql = "SELECT DISTINCT u.user_id, u.username, u.first_name FROM users u JOIN msg_meta m ON u.user_id = m.user_id LEFT JOIN here_ignore hi ON u.user_id = hi.user_id AND m.chat_id = hi.chat_id WHERE m.chat_id = $1 AND hi.user_id IS NULL"
    try:
        async with db_pool.acquire() as con: rows = await con.fetch(sql, chat_id)
        if not rows: await message.answer("👀 Всі сховалися."); return
        mentions = [f"@{r['username']}" if r['username'] else f"<a href='tg://user?id={r['user_id']}'>{r['first_name']}</a>" for r in rows]
        await message.answer("📢 <b>ОБЩИЙ СБОР</b>\n\n" + " ".join(mentions), parse_mode=ParseMode.HTML)
    except: await message.answer("Забагато людей.")

@dp.message(F.text.startswith('!say') & (F.chat.type == 'private'))
async def cmd_remote_say(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    try: await bot.send_message(chat_id=TARGET_CHAT_ID, text=message.text[5:].strip())
    except: pass

# ==========================================
# 🔥 НОВИЙ ХЕНДЛЕР: ОРЕ НА ТИХ, ХТО ТЕГАЄ УЗБЕКІВ
# ==========================================
@dp.message(F.entities & ~F.text.startswith('!'))
async def mention_only_handler(message: types.Message):
    await save_to_db(message)
    await check_for_sleeping_uzbeks(message)

# ==========================================
# УНІВЕРСАЛЬНИЙ GPT ХЕНДЛЕР (В КІНЦІ)
# ==========================================
@dp.message(F.text.startswith('!'))
async def cmd_universal_gpt(message: types.Message):
    await save_to_db(message)
    
    # 🔥 СПОЧАТКУ ПЕРЕВІРЯЄМО, ЧИ НЕ ТЕГНУЛИ УЗБЕКІВ
    await check_for_sleeping_uzbeks(message)
    
    if not gpt_client: return
    
    command_word = message.text.split()[0].lower()
    if command_word in ['!here', '!stats', '!roulette', '!system', '!clearsystem', '!temp', '!help', '!say', '!analyze', '!forget', '!models', '!model', '!ignorehere']:
        return

    prompt = message.text[1:].strip()
    if not prompt: return

    await bot.send_chat_action(chat_id=message.chat.id, action="typing")
    chat_id = message.chat.id
    bot_id = bot.id

    sys_prompt = DEFAULT_SYSTEM_PROMPT
    temperature = DEFAULT_TEMPERATURE
    model_to_use = DEFAULT_MODEL
    
    try:
        async with db_pool.acquire() as con:
            row = await con.fetchrow("SELECT system_prompt, temperature, model_name FROM chats WHERE chat_id=$1", chat_id)
            if row:
                if row['system_prompt']: sys_prompt = row['system_prompt']
                if row['temperature'] is not None: temperature = row['temperature']
                if row['model_name']: model_to_use = row['model_name']
    except Exception: pass

    messages_payload = []
    if sys_prompt:
        messages_payload.append({"role": "system", "content": sys_prompt})

    try:
        history_rows = await get_thread_context(chat_id, message.message_id)
        
        for row in history_rows:
            uid = row['user_id']
            text = row['msg_txt']
            name = row['first_name'] or "User"
            if uid == bot_id:
                messages_payload.append({"role": "assistant", "content": text})
            else:
                messages_payload.append({"role": "user", "content": f"{name}: {text}"})
    except Exception as e:
        logging.error(f"Context error: {e}")
        messages_payload.append({"role": "user", "content": prompt})

    try:
        response = await gpt_client.chat.completions.create(
            model=model_to_use, 
            messages=messages_payload,
            temperature=temperature
        )
        reply_text = response.choices[0].message.content
        await send_chunked_response(message, reply_text)

    except Exception as e:
        logging.error(f"OpenAI Error: {e}")
        await message.reply(f"Помилка AI: {e}")

# --- ЛОГЕР ---
@dp.message()
async def logger_handler(message: types.Message):
    if not message.from_user: return
    try: await save_to_db(message)
    except Exception as e: logging.error(f"DB Error: {e}")

# --- ЗАПУСК ---
async def main():
    await create_pool()
    print("🚀 Бот запущено!")
    try: await dp.start_polling(bot)
    finally:
        if db_pool: await db_pool.close(); print("✅ БД закрито.")

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: print("\n🛑 Вимкнено.")
    except Exception as e: print(f"❌ Error: {e}")