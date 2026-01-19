import asyncio
import logging
import os
import sys
import asyncpg
import random
import uuid
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import CallbackQuery, InputMediaPhoto
from dotenv import load_dotenv
from openai import AsyncOpenAI
from duckduckgo_search import DDGS

# --- КОНФІГУРАЦІЯ ---
load_dotenv()

API_TOKEN = os.getenv("BOT_TOKEN")
NEON_URL = os.getenv("NEON_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

ADMIN_ID = 548789253
TARGET_CHAT_ID = -1001981383150

THREAD_DEPTH_LIMIT = 10 

if not API_TOKEN or not NEON_URL:
    print("❌ ПОМИЛКА: Перевір .env файл")
    sys.exit(1)

logging.basicConfig(level=logging.INFO)

bot = Bot(token=API_TOKEN)
dp = Dispatcher()
db_pool = None

gpt_client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

DEFAULT_SYSTEM_PROMPT = None 
DEFAULT_TEMPERATURE = 1.0  
DEFAULT_MODEL = "gpt-5-mini" 

AVAILABLE_MODELS = [
    "gpt-5-mini",
    "gpt-5.2-chat-latest",
    "gpt-5-pro"
]

# Сховище результатів пошуку
SEARCH_RESULTS = {}

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

# --- ЛОГІКА ІСТОРІЇ ---
async def get_thread_context(chat_id, start_msg_id):
    async with db_pool.acquire() as con:
        sql = """
            WITH RECURSIVE thread AS (
                SELECT m.msg_id, m.reply_to, m.user_id, m.date_msg, 
                       t.msg_txt, p.photo_url as file_id, 1 as depth
                FROM msg_meta m
                LEFT JOIN msg_txt t ON m.chat_id = t.chat_id AND m.msg_id = t.msg_id
                LEFT JOIN photo p ON m.chat_id = p.chat_id AND m.msg_id = p.msg_id
                WHERE m.chat_id = $1 AND m.msg_id = $2
                UNION ALL
                SELECT parent.msg_id, parent.reply_to, parent.user_id, parent.date_msg, 
                       pt.msg_txt, pp.photo_url as file_id, thread.depth + 1
                FROM msg_meta parent
                LEFT JOIN msg_txt pt ON parent.chat_id = pt.chat_id AND parent.msg_id = pt.msg_id
                LEFT JOIN photo pp ON parent.chat_id = pp.chat_id AND parent.msg_id = pp.msg_id
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

async def get_image_url(file_id):
    try:
        file = await bot.get_file(file_id)
        return f"https://api.telegram.org/file/bot{API_TOKEN}/{file.file_path}"
    except Exception as e:
        logging.error(f"Error getting file url: {e}")
        return None

async def check_for_sleeping_uzbeks(message: types.Message):
    if not message.entities: return
    mentioned_ids = []
    mentioned_usernames = []
    for entity in message.entities:
        if entity.type == 'text_mention': mentioned_ids.append(entity.user.id)
        elif entity.type == 'mention': mentioned_usernames.append(message.text[entity.offset + 1 : entity.offset + entity.length])
    if not mentioned_ids and not mentioned_usernames: return
    chat_id = message.chat.id
    try:
        async with db_pool.acquire() as con:
            exists = await con.fetchval("SELECT 1 FROM here_ignore hi LEFT JOIN users u ON hi.user_id = u.user_id WHERE hi.chat_id = $1 AND (hi.user_id = ANY($2) OR u.username = ANY($3)) LIMIT 1", chat_id, mentioned_ids, mentioned_usernames)
            if exists: await message.reply("ЧШШШШ УЗБЕКІ СПЯТЬ")
    except: pass

async def send_chunked_response(message_obj, text):
    async def send_safe(chunk):
        try: return await message_obj.answer(chunk, parse_mode=ParseMode.MARKDOWN)
        except: 
            try: return await message_obj.answer(chunk, parse_mode=None)
            except: return None
    if len(text) <= 4000: sent = await send_safe(text); 
    else:
        for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]: await send_safe(chunk)

def get_cutoff_date(p):
    n = datetime.utcnow()
    return n-timedelta(days=1) if p=='1d' else n-timedelta(days=7) if p=='7d' else n-timedelta(days=30) if p=='30d' else None

def get_period_name(p): return "24 години" if p=='1d' else "7 днів" if p=='7d' else "30 днів" if p=='30d' else "Весь час"

# --- НАЛАШТУВАННЯ ---
@dp.message(F.text.startswith('!system'))
async def cmd_set_system(m: types.Message):
    await save_to_db(m); t = m.text[8:].strip()
    if not t:
        async with db_pool.acquire() as con: cur = await con.fetchval("SELECT system_prompt FROM chats WHERE chat_id=$1", m.chat.id)
        await m.answer(f"🧠 <b>Установка:</b>\n<code>{cur if cur else '(Пусто)'}</code>", parse_mode=ParseMode.HTML); return
    async with db_pool.acquire() as con: await con.execute("UPDATE chats SET system_prompt=$1 WHERE chat_id=$2", t, m.chat.id)
    await m.answer(f"✅ <b>Нова особистість:</b> {t}", parse_mode=ParseMode.HTML)

@dp.message(F.text.lower().startswith('!clearsystem'))
async def cmd_clear_system(m: types.Message):
    await save_to_db(m)
    async with db_pool.acquire() as con: await con.execute("UPDATE chats SET system_prompt=NULL WHERE chat_id=$1", m.chat.id)
    await m.answer("🔄 <b>Скинуто.</b>", parse_mode=ParseMode.HTML)

@dp.message(F.text.lower().startswith('!forget'))
async def cmd_forget(m: types.Message): await save_to_db(m); await m.answer("🧹 <b>Контекст 'освіжено'.</b>", parse_mode=ParseMode.HTML)

@dp.message(F.text.lower().startswith('!temp'))
async def cmd_set_temp(m: types.Message):
    await save_to_db(m); args = m.text.split()
    if len(args)<2:
        async with db_pool.acquire() as con: cur = await con.fetchval("SELECT temperature FROM chats WHERE chat_id=$1", m.chat.id)
        await m.answer(f"🌡 <b>Температура:</b> {cur if cur is not None else DEFAULT_TEMPERATURE}", parse_mode=ParseMode.HTML); return
    try:
        val = float(args[1])
        if not (0<=val<=2): await m.answer("❌ 0.0 - 2.0"); return
        async with db_pool.acquire() as con: await con.execute("UPDATE chats SET temperature=$1 WHERE chat_id=$2", val, m.chat.id)
        await m.answer(f"🌡 <b>Встановлено:</b> {val}", parse_mode=ParseMode.HTML)
    except: await m.answer("❌ Число треба.")

@dp.message(F.text.lower().startswith('!models') | F.text.lower().startswith('!model'))
async def cmd_models_menu(m: types.Message):
    await save_to_db(m)
    async with db_pool.acquire() as con: cur = await con.fetchval("SELECT model_name FROM chats WHERE chat_id=$1", m.chat.id)
    cur = cur if cur else DEFAULT_MODEL
    b = InlineKeyboardBuilder()
    for mod in AVAILABLE_MODELS: b.button(text=f"✅ {mod}" if mod==cur else mod, callback_data=f"set_mdl_{mod}")
    b.adjust(1)
    await m.answer(f"💾 <b>Поточна модель:</b> <code>{cur}</code>", parse_mode=ParseMode.HTML, reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("set_mdl_"))
async def cb_set_model(c: CallbackQuery):
    mod = c.data.replace("set_mdl_", "")
    if mod not in AVAILABLE_MODELS: await c.answer("❌ Нема такої.", show_alert=True); return
    async with db_pool.acquire() as con: await con.execute("UPDATE chats SET model_name=$1 WHERE chat_id=$2", mod, c.message.chat.id)
    await c.message.edit_text(f"💾 <b>Встановлено:</b> <code>{mod}</code>", parse_mode=ParseMode.HTML)

# --- ANALYZE ---
@dp.message(F.text.lower().startswith('!analyze'))
async def cmd_analyze_menu(m: types.Message):
    await save_to_db(m)
    sql="SELECT u.user_id, u.first_name, COUNT(m.msg_id) as cnt FROM msg_meta m JOIN users u ON m.user_id=u.user_id JOIN msg_txt t ON m.chat_id=t.chat_id AND m.msg_id=t.msg_id WHERE m.chat_id=$1 GROUP BY u.user_id, u.first_name ORDER BY cnt DESC LIMIT 20"
    async with db_pool.acquire() as con: rows = await con.fetch(sql, m.chat.id)
    if not rows: await m.answer("Пусто."); return
    b = InlineKeyboardBuilder()
    for r in rows: b.button(text=r['first_name'], callback_data=f"anlz_sel_{r['user_id']}")
    b.adjust(2)
    await m.answer("🕵️‍♂️ <b>Кого?</b>", parse_mode=ParseMode.HTML, reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("anlz_sel_"))
async def cb_sel(c: CallbackQuery):
    uid = c.data.split("_")[2]
    b = InlineKeyboardBuilder()
    for n in [25, 50, 100]: b.button(text=f"📝 {n}", callback_data=f"anlz_run_{uid}_{n}")
    b.button(text="📝 1000 (VIP)", callback_data=f"anlz_run_{uid}_1000")
    b.adjust(2)
    await c.message.edit_text("🔢 <b>Скільки?</b>", parse_mode=ParseMode.HTML, reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("anlz_run_"))
async def cb_run(c: CallbackQuery):
    _, _, uid, lim = c.data.split("_"); uid, lim = int(uid), int(lim)
    if lim==1000 and c.from_user.id!=ADMIN_ID:
        async with db_pool.acquire() as con:
            lr = await con.fetchval("SELECT last_1000_analyze FROM users WHERE user_id=$1", c.from_user.id)
            if lr and datetime.utcnow() < (nxt:=lr+timedelta(hours=24)):
                d=nxt-datetime.utcnow(); await c.answer(f"⛔ Чекай {d.seconds//3600}год", show_alert=True); return
            await con.execute("UPDATE users SET last_1000_analyze=$1 WHERE user_id=$2", datetime.utcnow(), c.from_user.id)
    await c.message.edit_text("⏳ <b>Думаю...</b>", parse_mode=ParseMode.HTML)
    sql="SELECT t.msg_txt FROM msg_meta m JOIN msg_txt t ON m.chat_id=t.chat_id AND m.msg_id=t.msg_id WHERE m.chat_id=$1 AND m.user_id=$2 AND t.msg_txt!='' ORDER BY m.date_msg DESC LIMIT $3"
    async with db_pool.acquire() as con:
        rows = await con.fetch(sql, c.message.chat.id, uid, lim)
        uname = await con.fetchval("SELECT first_name FROM users WHERE user_id=$1", uid)
        mod = await con.fetchval("SELECT model_name FROM chats WHERE chat_id=$1", c.message.chat.id)
    txts = [r['msg_txt'] for r in rows if r['msg_txt'].strip()]
    if not txts: await c.message.edit_text("❌ Пусто."); return
    try:
        resp = await gpt_client.chat.completions.create(model=mod or DEFAULT_MODEL, messages=[{"role":"system","content":"Психоаналітик."},{"role":"user","content":f"Проаналізуй {uname}:\n"+"\n".join(txts)}])
        await c.message.delete()
        await send_chunked_response(c.message, f"🧠 <b>Аналіз {uname}:</b>\n\n{resp.choices[0].message.content}")
    except Exception as e: await c.message.answer(f"Error: {e}")

# --- STATS, IGNORE, ETC ---
@dp.message(F.text.lower().startswith('!stats'))
async def cmd_st(m: types.Message):
    await save_to_db(m); b=InlineKeyboardBuilder(); b.button(text="Група",callback_data="ask_period_group"); b.button(text="Юзер",callback_data="ask_period_user"); await m.answer("Чию?", reply_markup=b.as_markup())

@dp.callback_query(F.data=="stats_main_menu")
async def cb_back(c: CallbackQuery): await cmd_st(c.message)

@dp.callback_query(F.data.startswith("ask_period_"))
async def cb_per(c: CallbackQuery):
    p = c.data.split("_")[2]
    b=InlineKeyboardBuilder()
    for t,v in [("1д","1d"),("7д","7d"),("30д","30d"),("Все","all")]: b.button(text=t,callback_data=f"res_grp_{v}" if p=="group" else f"list_usr_{v}")
    b.button(text="🔙",callback_data="stats_main_menu"); await c.message.edit_text("Період?", reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("res_grp_"))
async def cb_res_g(c: CallbackQuery):
    per = c.data.split("_")[2]; cut = get_cutoff_date(per)
    sql_c = "SELECT COUNT(*) FROM msg_meta WHERE chat_id=$1" + (" AND date_msg>=$2" if cut else "")
    sql_t = "SELECT u.first_name, COUNT(m.msg_id) as cnt FROM msg_meta m JOIN users u ON m.user_id=u.user_id WHERE m.chat_id=$1" + (" AND m.date_msg>=$2" if cut else "") + " GROUP BY u.first_name ORDER BY cnt DESC LIMIT 5"
    async with db_pool.acquire() as con:
        tot = await con.fetchval(sql_c, c.message.chat.id, *([cut] if cut else []))
        top = await con.fetch(sql_t, c.message.chat.id, *([cut] if cut else []))
    txt = f"📊 <b>Стат ({get_period_name(per)})</b>: {tot}\n" + "\n".join([f"{i+1}. {r['first_name']} - {r['cnt']}" for i,r in enumerate(top)])
    b=InlineKeyboardBuilder(); b.button(text="🔙",callback_data="ask_period_group"); await c.message.edit_text(txt, parse_mode=ParseMode.HTML, reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("list_usr_"))
async def cb_lst(c: CallbackQuery):
    per=c.data.split("_")[2]; cut=get_cutoff_date(per)
    sql="SELECT u.user_id, u.first_name, COUNT(m.msg_id) as cnt FROM msg_meta m JOIN users u ON m.user_id=u.user_id WHERE m.chat_id=$1" + (" AND m.date_msg>=$2" if cut else "") + " GROUP BY u.user_id, u.first_name ORDER BY cnt DESC LIMIT 20"
    async with db_pool.acquire() as con: rows=await con.fetch(sql, c.message.chat.id, *([cut] if cut else []))
    b=InlineKeyboardBuilder()
    for r in rows: b.button(text=r['first_name'], callback_data=f"stat_u_{r['user_id']}_{per}")
    b.button(text="🔙",callback_data="ask_period_user"); await c.message.edit_text("Топ:", reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("stat_u_"))
async def cb_u_det(c: CallbackQuery):
    _, _, uid, per = c.data.split("_"); uid=int(uid); cut=get_cutoff_date(per)
    async with db_pool.acquire() as con:
        n = await con.fetchval("SELECT first_name FROM users WHERE user_id=$1", uid)
        tot = await con.fetchval("SELECT COUNT(*) FROM msg_meta WHERE chat_id=$1 AND user_id=$2" + (" AND date_msg>=$3" if cut else ""), c.message.chat.id, uid, *([cut] if cut else []))
        sts = await con.fetch("SELECT msg_type, COUNT(*) as cnt FROM msg_meta WHERE chat_id=$1 AND user_id=$2" + (" AND date_msg>=$3" if cut else "") + " GROUP BY msg_type ORDER BY cnt DESC", c.message.chat.id, uid, *([cut] if cut else []))
    txt = f"👤 <b>{n}</b> ({get_period_name(per)})\n📨 {tot}\n" + "\n".join([f"🔹 {r['msg_type']}: {r['cnt']}" for r in sts])
    b=InlineKeyboardBuilder(); b.button(text="🔙",callback_data=f"list_usr_{per}"); await c.message.edit_text(txt, parse_mode=ParseMode.HTML, reply_markup=b.as_markup())

@dp.message(F.text.lower().startswith('!ignorehere'))
async def cmd_ign(m: types.Message):
    await save_to_db(m)
    async with db_pool.acquire() as con:
        if await con.execute("DELETE FROM here_ignore WHERE chat_id=$1 AND user_id=$2", m.chat.id, m.from_user.id) == "DELETE 1":
            await m.answer("👻 <b>Ти в грі.</b>", parse_mode=ParseMode.HTML)
        else:
            await con.execute("INSERT INTO here_ignore (chat_id, user_id) VALUES ($1, $2)", m.chat.id, m.from_user.id)
            await m.answer("🔕 <b>Ігнор увімкнено.</b>", parse_mode=ParseMode.HTML)

@dp.message(F.text.lower().startswith('!roulette'))
async def cmd_rl(m: types.Message):
    if m.chat.type=='private': return
    await save_to_db(m)
    sql="SELECT * FROM (SELECT DISTINCT u.user_id, u.username, u.first_name FROM users u JOIN msg_meta m ON u.user_id=m.user_id LEFT JOIN here_ignore hi ON u.user_id=hi.user_id AND m.chat_id=hi.chat_id WHERE m.chat_id=$1 AND hi.user_id IS NULL) t ORDER BY RANDOM() LIMIT 1"
    async with db_pool.acquire() as con: r=await con.fetchrow(sql, m.chat.id)
    if r: await m.answer(f"{'@'+r['username'] if r['username'] else r['first_name']} - НУ ТИ І ПІДАРАС", parse_mode=ParseMode.HTML)

@dp.message(F.text.lower().startswith('!here'))
async def cmd_hr(m: types.Message):
    await save_to_db(m)
    sql="SELECT DISTINCT u.user_id, u.username, u.first_name FROM users u JOIN msg_meta m ON u.user_id=m.user_id LEFT JOIN here_ignore hi ON u.user_id=hi.user_id AND m.chat_id=hi.chat_id WHERE m.chat_id=$1 AND hi.user_id IS NULL"
    async with db_pool.acquire() as con: rows=await con.fetch(sql, m.chat.id)
    lst=[f"@{r['username']}" if r['username'] else f"<a href='tg://user?id={r['user_id']}'>{r['first_name']}</a>" for r in rows]
    await m.answer("📢 <b>ЗБІР</b>\n"+(" ".join(lst) if lst else "Всі в ігнорі"), parse_mode=ParseMode.HTML)

@dp.message(F.text.lower().startswith('!help'))
async def cmd_h(m: types.Message): await save_to_db(m); await m.answer("Команди: !текст, !models, !psearch, !analyze, !system, !forget, !temp, !stats, !here, !ignorehere, !roulette", parse_mode=ParseMode.HTML)

@dp.message(F.text.startswith('!say') & (F.chat.type=='private'))
async def cmd_say(m: types.Message): 
    if m.from_user.id==ADMIN_ID: await bot.send_message(TARGET_CHAT_ID, m.text[5:].strip())

@dp.message(F.entities & ~F.text.startswith('!'))
async def ment_h(m: types.Message): await save_to_db(m); await check_for_sleeping_uzbeks(m)

# --- 🔥 КОМАНДА !psearch (ГАЛЕРЕЯ) ---

def get_psearch_keyboard(search_id, current_index, total_count):
    builder = InlineKeyboardBuilder()
    builder.button(text="⏪", callback_data=f"ps_nav_{search_id}_0")
    prev_idx = max(0, current_index - 1)
    builder.button(text="⬅️", callback_data=f"ps_nav_{search_id}_{prev_idx}")
    builder.button(text=f"{current_index + 1}/{total_count}", callback_data="noop")
    next_idx = min(total_count - 1, current_index + 1)
    builder.button(text="➡️", callback_data=f"ps_nav_{search_id}_{next_idx}")
    builder.button(text="⏩", callback_data=f"ps_nav_{search_id}_{total_count - 1}")
    builder.adjust(5)
    return builder.as_markup()

@dp.message(F.text.lower().startswith('!psearch'))
async def cmd_psearch(message: types.Message):
    await save_to_db(message)
    query = message.text[8:].strip()
    if not query:
        await message.reply("Введи запит. Наприклад: `!psearch котик`", parse_mode=ParseMode.MARKDOWN)
        return

    await message.bot.send_chat_action(chat_id=message.chat.id, action="upload_photo")

    try:
        # 🔥 ЛІМІТ 25 - ЦЕ МАКСИМУМ ДЛЯ БЕЗКОШТОВНОГО API
        # safesearch='off' (без цензури)
        with DDGS() as ddgs:
            results = list(ddgs.images(query, max_results=25, safesearch='off'))
        
        if not results:
            await message.reply("На жаль, нічого не знайдено.")
            return

        search_id = str(uuid.uuid4())[:8]
        image_urls = [res.get('image') for res in results if res.get('image')]
        
        if not image_urls:
            await message.reply("Знайшов результати, але без посилань на фото.")
            return

        SEARCH_RESULTS[search_id] = image_urls
        first_url = image_urls[0]
        markup = get_psearch_keyboard(search_id, 0, len(image_urls))
        
        await message.reply_photo(photo=first_url, caption=f"🔎 <b>{query}</b>", reply_markup=markup, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logging.error(f"Psearch error: {e}")
        await message.reply(f"Помилка пошуку (Rate Limit або IP блок): {e}")

@dp.callback_query(F.data.startswith("ps_nav_"))
async def cb_psearch_nav(callback: CallbackQuery):
    try:
        _, _, search_id, idx_str = callback.data.split("_")
        index = int(idx_str)
        images = SEARCH_RESULTS.get(search_id)
        if not images:
            await callback.answer("⚠️ Цей пошук застарів.", show_alert=True)
            return

        if index < 0: index = 0
        if index >= len(images): index = len(images) - 1

        img_url = images[index]
        markup = get_psearch_keyboard(search_id, index, len(images))
        media = InputMediaPhoto(media=img_url, caption=callback.message.caption, parse_mode=ParseMode.HTML)
        try: await callback.message.edit_media(media=media, reply_markup=markup)
        except Exception as e:
            if "message is not modified" in str(e): await callback.answer()
            else: await callback.answer("⚠️ Не вдалося завантажити.", show_alert=True)
                
    except Exception as e:
        logging.error(f"Psearch nav error: {e}")
        await callback.answer("Помилка навігації.")

@dp.callback_query(F.data == "noop")
async def cb_noop(callback: CallbackQuery): await callback.answer()

# --- 🔥 УНІВЕРСАЛЬНИЙ GPT (МУЛЬТИМОДАЛЬНИЙ) ---
@dp.message(F.text.startswith('!') | (F.caption & F.caption.startswith('!')))
async def cmd_gpt(message: types.Message):
    await save_to_db(message)
    await check_for_sleeping_uzbeks(message)
    if not gpt_client: return

    full_text = message.text or message.caption or ""
    command_word = full_text.split()[0].lower()
    
    if command_word in ['!here', '!stats', '!roulette', '!system', '!clearsystem', '!temp', '!help', '!say', '!analyze', '!forget', '!models', '!model', '!ignorehere', '!psearch']:
        return

    prompt = full_text[1:].strip()
    if not prompt and not message.photo: return

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
    except: pass

    messages_payload = []
    if sys_prompt: messages_payload.append({"role": "system", "content": sys_prompt})

    try:
        history_rows = await get_thread_context(chat_id, message.message_id)
        for row in history_rows:
            uid, text_content, file_id, name = row['user_id'], row['msg_txt'], row['file_id'], row['first_name'] or "User"
            content_block = []
            if text_content:
                final_text = text_content if uid == bot_id else f"{name}: {text_content}"
                content_block.append({"type": "text", "text": final_text})
            if file_id:
                img_url = await get_image_url(file_id)
                if img_url: content_block.append({"type": "image_url", "image_url": {"url": img_url}})
            if content_block:
                messages_payload.append({"role": "assistant" if uid == bot_id else "user", "content": content_block})
                
    except Exception as e:
        messages_payload.append({"role": "user", "content": prompt})

    try:
        response = await gpt_client.chat.completions.create(model=model_to_use, messages=messages_payload, temperature=temperature)
        await send_chunked_response(message, response.choices[0].message.content)
    except Exception as e:
        await message.reply(f"Помилка AI: {e}")

# --- ЛОГЕР ---
@dp.message()
async def logger_handler(message: types.Message):
    if not message.from_user: return
    try: await save_to_db(message)
    except: pass

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