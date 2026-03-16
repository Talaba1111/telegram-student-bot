import asyncio
import json
import logging
import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import gspread
from gspread.cell import Cell
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatMemberStatus
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "")
REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL", "").strip()

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi")
if not GOOGLE_SHEET_NAME:
    raise ValueError("GOOGLE_SHEET_NAME topilmadi")
if not GOOGLE_CREDENTIALS_JSON:
    raise ValueError("GOOGLE_CREDENTIALS_JSON topilmadi")

ADMIN_IDS = {int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip().isdigit()}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
user_locks = defaultdict(asyncio.Lock)


class FormState(StatesGroup):
    choosing_education = State()
    choosing_course = State()
    choosing_group = State()
    choosing_student = State()
    waiting_main_phone = State()
    waiting_extra_phone = State()
    confirm_save = State()
    confirm_edit = State()
    recover_main = State()
    recover_extra = State()


def norm(v) -> str:
    return str(v).strip() if v is not None else ""


def norm_header(v: str) -> str:
    return norm(v).lower().replace("’", "'").replace("`", "'").replace("ʻ", "'")


def norm_phone(v: str) -> str:
    v = re.sub(r"[^\d+]", "", norm(v))
    if v.startswith("998") and not v.startswith("+"):
        v = "+" + v
    return v


def valid_phone(v: str) -> bool:
    return bool(re.fullmatch(r"^\+998\d{9}$", v))


def unique(values: List[str]) -> List[str]:
    res = []
    for v in values:
        v = norm(v)
        if v and v not in res:
            res.append(v)
    return res


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def paginated_keyboard(
    prefix: str,
    items: List[str],
    page: int = 0,
    page_size: int = 8,
    row_width: int = 2,
    back_cb: Optional[str] = None,
) -> InlineKeyboardMarkup:
    total_pages = max(1, (len(items) + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    end = start + page_size
    page_items = items[start:end]

    rows = []
    row = []
    for idx, item in enumerate(page_items, start=start):
        row.append(InlineKeyboardButton(text=item, callback_data=f"{prefix}|{idx}"))
        if len(row) == row_width:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"{prefix}_page|{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"{prefix}_page|{page+1}"))
        rows.append(nav)

    bottom = []
    if back_cb:
        bottom.append(InlineKeyboardButton(text="🔙 Orqaga", callback_data=back_cb))
    bottom.append(InlineKeyboardButton(text="❌ Bekor qilish", callback_data="cancel"))
    rows.append(bottom)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Tasdiqlash", callback_data="save_yes"),
                InlineKeyboardButton(text="✏️ Qayta kiritish", callback_data="save_rewrite"),
            ],
            [InlineKeyboardButton(text="❌ Bekor qilish", callback_data="cancel")],
        ]
    )


def existing_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📱 Asosiy raqamni o'zgartirish", callback_data="edit_main")],
            [InlineKeyboardButton(text="☎️ Qo'shimcha raqamni o'zgartirish", callback_data="edit_extra")],
            [InlineKeyboardButton(text="🔁 Ikkalasini qayta kiritish", callback_data="edit_both")],
            [InlineKeyboardButton(text="❌ Bekor qilish", callback_data="cancel")],
        ]
    )


async def safe_edit(callback: CallbackQuery, text: str, markup: Optional[InlineKeyboardMarkup] = None):
    if not callback.message:
        await callback.answer()
        return

    current_text = callback.message.text or callback.message.caption or ""
    try:
        if current_text == text and callback.message.reply_markup == markup:
            await callback.answer()
            return

        if current_text == text:
            await callback.message.edit_reply_markup(reply_markup=markup)
        else:
            await callback.message.edit_text(text, reply_markup=markup)

        await callback.answer()
    except Exception as e:
        if "message is not modified" in str(e).lower():
            await callback.answer()
            return
        raise


async def check_subscription(user_id: int) -> bool:
    if not REQUIRED_CHANNEL:
        return True
    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL, user_id)
        return member.status in {
            ChatMemberStatus.CREATOR,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.MEMBER,
        }
    except Exception:
        return False


def get_credentials():
    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    if "private_key" in creds:
        creds["private_key"] = creds["private_key"].replace("\\n", "\n")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_info(creds, scopes=scopes)


def get_worksheet_sync():
    client = gspread.authorize(get_credentials())
    return client.open(GOOGLE_SHEET_NAME).sheet1


async def get_worksheet():
    try:
        return await asyncio.to_thread(get_worksheet_sync)
    except Exception as e:
        raise ValueError(f"Google Sheets ulanishida xatolik: {e}")


async def get_headers(ws) -> List[str]:
    return await asyncio.to_thread(lambda: [str(h).strip() for h in ws.row_values(1)])


async def get_all_records(ws):
    return await asyncio.to_thread(ws.get_all_records)


async def get_col_name(ws, possible_names: List[str]) -> Optional[str]:
    headers = await get_headers(ws)
    mapping = {norm_header(h): h for h in headers}
    for name in possible_names:
        key = norm_header(name)
        if key in mapping:
            return mapping[key]
    return None


async def get_col_index(ws, col_name: str) -> Optional[int]:
    headers = await get_headers(ws)
    for i, h in enumerate(headers, start=1):
        if norm_header(h) == norm_header(col_name):
            return i
    return None


async def ensure_extra_columns(ws):
    headers = await get_headers(ws)
    needed = [
        "Asosiy nomer",
        "Qo'shimcha nomer",
        "Telegram ID",
        "Telegram Username",
        "Telegram Full Name",
        "Oxirgi yangilanish",
        "Yuborish soni",
    ]
    current = headers[:]
    for col in needed:
        if col not in current:
            await asyncio.to_thread(ws.update_cell, 1, len(current) + 1, col)
            current.append(col)


async def get_required_columns(ws) -> Dict[str, str]:
    education = await get_col_name(ws, ["Ta'lim shakli", "Ta’lim shakli", "Talim shakli"])
    course = await get_col_name(ws, ["Kurs"])
    group = await get_col_name(ws, ["Guruh", "Group"])
    student = await get_col_name(ws, ["F.I.SH.", "F.I.SH", "FISH", "FIO", "Talaba"])

    if not education:
        raise ValueError("Google Sheetsda Ta'lim shakli ustuni topilmadi")
    if not course:
        raise ValueError("Google Sheetsda Kurs ustuni topilmadi")
    if not group:
        raise ValueError("Google Sheetsda Guruh ustuni topilmadi")
    if not student:
        raise ValueError("Google Sheetsda F.I.SH. ustuni topilmadi")

    return {"education": education, "course": course, "group": group, "student": student}


async def fetch_snapshot() -> Dict:
    ws = await get_worksheet()
    await ensure_extra_columns(ws)
    records = await get_all_records(ws)
    cols = await get_required_columns(ws)
    return {"records": records, "columns": cols}


def snapshot_educations(snap: Dict) -> List[str]:
    cols = snap["columns"]
    return unique([norm(r.get(cols["education"], "")) for r in snap["records"]])


def snapshot_courses(snap: Dict, education: str) -> List[str]:
    cols = snap["columns"]
    return unique([
        norm(r.get(cols["course"], ""))
        for r in snap["records"]
        if norm(r.get(cols["education"], "")) == education
    ])


def snapshot_groups(snap: Dict, education: str, course: str) -> List[str]:
    cols = snap["columns"]
    return unique([
        norm(r.get(cols["group"], ""))
        for r in snap["records"]
        if norm(r.get(cols["education"], "")) == education
        and norm(r.get(cols["course"], "")) == course
    ])


def snapshot_students(snap: Dict, education: str, course: str, group: str) -> List[str]:
    cols = snap["columns"]
    return unique([
        norm(r.get(cols["student"], ""))
        for r in snap["records"]
        if norm(r.get(cols["education"], "")) == education
        and norm(r.get(cols["course"], "")) == course
        and norm(r.get(cols["group"], "")) == group
    ])


def registration_by_tg_id(snap: Dict, tg_id: str) -> Optional[Dict[str, str]]:
    cols = snap["columns"]
    for row in snap["records"]:
        if norm(row.get("Telegram ID", "")) == tg_id:
            return {
                "student": norm(row.get(cols["student"], "")),
                "course": norm(row.get(cols["course"], "")),
                "group": norm(row.get(cols["group"], "")),
                "education": norm(row.get(cols["education"], "")),
                "main_phone": norm(row.get("Asosiy nomer", "")),
                "extra_phone": norm(row.get("Qo'shimcha nomer", "")),
                "telegram_id": norm(row.get("Telegram ID", "")),
            }
    return None


def registration_by_phones(snap: Dict, main_phone: str, extra_phone: str) -> Optional[Dict[str, str]]:
    cols = snap["columns"]
    for row in snap["records"]:
        if norm(row.get("Asosiy nomer", "")) == main_phone and norm(row.get("Qo'shimcha nomer", "")) == extra_phone:
            return {
                "student": norm(row.get(cols["student"], "")),
                "course": norm(row.get(cols["course"], "")),
                "group": norm(row.get(cols["group"], "")),
                "education": norm(row.get(cols["education"], "")),
                "main_phone": norm(row.get("Asosiy nomer", "")),
                "extra_phone": norm(row.get("Qo'shimcha nomer", "")),
                "telegram_id": norm(row.get("Telegram ID", "")),
            }
    return None


async def find_row_index(ws, student: str, course: str, group: str, education: str) -> Optional[int]:
    rows = await get_all_records(ws)
    cols = await get_required_columns(ws)
    for i, row in enumerate(rows, start=2):
        if (
            norm(row.get(cols["student"], "")) == student
            and norm(row.get(cols["course"], "")) == course
            and norm(row.get(cols["group"], "")) == group
            and norm(row.get(cols["education"], "")) == education
        ):
            return i
    return None


async def save_registration(
    student: str,
    course: str,
    group: str,
    education: str,
    main_phone: str,
    extra_phone: str,
    telegram_id: str,
    telegram_username: str,
    telegram_full_name: str,
):
    ws = await get_worksheet()
    await ensure_extra_columns(ws)
    row_index = await find_row_index(ws, student, course, group, education)
    if not row_index:
        raise ValueError("Tanlangan talaba jadvaldan topilmadi")

    rows = await get_all_records(ws)
    cols = await get_required_columns(ws)

    for i, row in enumerate(rows, start=2):
        row_tg_id = norm(row.get("Telegram ID", ""))
        if row_tg_id == telegram_id and i != row_index:
            student_name = norm(row.get(cols["student"], ""))
            raise ValueError(f"Bu Telegram ID boshqa talabaga biriktirilgan: {student_name}")

    main_col = await get_col_index(ws, "Asosiy nomer")
    extra_col = await get_col_index(ws, "Qo'shimcha nomer")
    tg_id_col = await get_col_index(ws, "Telegram ID")
    tg_user_col = await get_col_index(ws, "Telegram Username")
    tg_name_col = await get_col_index(ws, "Telegram Full Name")
    upd_col = await get_col_index(ws, "Oxirgi yangilanish")
    cnt_col = await get_col_index(ws, "Yuborish soni")

    existing_main = norm((await asyncio.to_thread(ws.cell, row_index, main_col)).value) if main_col else ""
    existing_extra = norm((await asyncio.to_thread(ws.cell, row_index, extra_col)).value) if extra_col else ""
    current_count = 0
    if cnt_col:
        val = (await asyncio.to_thread(ws.cell, row_index, cnt_col)).value
        if val:
            try:
                current_count = int(str(val).strip())
            except Exception:
                current_count = 0

    if existing_main == main_phone and existing_extra == extra_phone:
        raise ValueError("Bu ma'lumot avval ham saqlangan")

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cells = []
    if main_col:
        cells.append(Cell(row_index, main_col, main_phone))
    if extra_col:
        cells.append(Cell(row_index, extra_col, extra_phone))
    if tg_id_col:
        cells.append(Cell(row_index, tg_id_col, telegram_id))
    if tg_user_col:
        cells.append(Cell(row_index, tg_user_col, telegram_username))
    if tg_name_col:
        cells.append(Cell(row_index, tg_name_col, telegram_full_name))
    if upd_col:
        cells.append(Cell(row_index, upd_col, now_str))
    if cnt_col:
        cells.append(Cell(row_index, cnt_col, str(current_count + 1)))

    if cells:
        await asyncio.to_thread(ws.update_cells, cells)


async def rebind_account(main_phone: str, extra_phone: str, telegram_id: str, username: str, full_name: str):
    ws = await get_worksheet()
    await ensure_extra_columns(ws)
    rows = await get_all_records(ws)
    cols = await get_required_columns(ws)

    target_index = None
    for i, row in enumerate(rows, start=2):
        if norm(row.get("Asosiy nomer", "")) == main_phone and norm(row.get("Qo'shimcha nomer", "")) == extra_phone:
            target_index = i
            break

    if not target_index:
        raise ValueError("Bunday raqamlar juftligi topilmadi")

    for i, row in enumerate(rows, start=2):
        row_tg_id = norm(row.get("Telegram ID", ""))
        if row_tg_id == telegram_id and i != target_index:
            student_name = norm(row.get(cols["student"], ""))
            raise ValueError(f"Bu Telegram ID boshqa talabaga biriktirilgan: {student_name}")

    tg_id_col = await get_col_index(ws, "Telegram ID")
    tg_user_col = await get_col_index(ws, "Telegram Username")
    tg_name_col = await get_col_index(ws, "Telegram Full Name")
    upd_col = await get_col_index(ws, "Oxirgi yangilanish")

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cells = []
    if tg_id_col:
        cells.append(Cell(target_index, tg_id_col, telegram_id))
    if tg_user_col:
        cells.append(Cell(target_index, tg_user_col, username))
    if tg_name_col:
        cells.append(Cell(target_index, tg_name_col, full_name))
    if upd_col:
        cells.append(Cell(target_index, upd_col, now_str))

    if cells:
        await asyncio.to_thread(ws.update_cells, cells)


async def show_confirm(message: Message, state: FSMContext):
    data = await state.get_data()
    text = (
        "📝 Kiritilgan ma'lumotlarni tekshiring:\n\n"
        f"🎓 Ta'lim shakli: {data.get('education', '')}\n"
        f"📚 Kurs: {data.get('course', '')}\n"
        f"👥 Guruh: {data.get('group', '')}\n"
        f"🧑‍🎓 Talaba: {data.get('student', '')}\n"
        f"📱 Asosiy raqam: {data.get('main_phone', '')}\n"
        f"☎️ Qo'shimcha raqam: {data.get('extra_phone', '')}\n\n"
        "Tasdiqlaysizmi?"
    )
    await message.answer(text, reply_markup=confirm_keyboard())
    await state.set_state(FormState.confirm_save)


async def cancel_flow(state: FSMContext, message: Optional[Message] = None, callback: Optional[CallbackQuery] = None):
    await state.clear()
    text = "❌ Amal bekor qilindi.\nQayta boshlash uchun /start bosing."
    if callback and callback.message:
        await safe_edit(callback, text, None)
    elif message:
        await message.answer(text, reply_markup=ReplyKeyboardRemove())


# =========================
# USER COMMANDS
# =========================
@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    try:
        if not message.from_user:
            await message.answer("❌ Foydalanuvchi aniqlanmadi.")
            return

        if not await check_subscription(message.from_user.id):
            await message.answer(
                "📢 Botdan foydalanish uchun avval kanalga obuna bo‘ling.\n\n"
                f"🔗 Kanal: {REQUIRED_CHANNEL}\n\n"
                "✅ Obuna bo‘lgach, qayta /start bosing."
            )
            return

        snap = await fetch_snapshot()
        existing = registration_by_tg_id(snap, str(message.from_user.id))

        if existing:
            await state.clear()
            await state.update_data(
                sheet_snapshot=snap,
                student=existing["student"],
                course=existing["course"],
                group=existing["group"],
                education=existing["education"],
                main_phone=existing["main_phone"],
                extra_phone=existing["extra_phone"],
            )
            await message.answer(
                "ℹ️ Siz avval ro'yxatdan o'tgansiz.\n\n"
                f"🎓 Ta'lim shakli: {existing['education']}\n"
                f"📚 Kurs: {existing['course']}\n"
                f"👥 Guruh: {existing['group']}\n"
                f"🧑‍🎓 Talaba: {existing['student']}\n"
                f"📱 Asosiy raqam: {existing['main_phone'] or '-'}\n"
                f"☎️ Qo'shimcha raqam: {existing['extra_phone'] or '-'}\n\n"
                "Siz faqat o'zingizning raqamlaringizni o'zgartira olasiz.\n"
                "Akkaunt o'zgargan bo'lsa /recover ishlating.",
                reply_markup=existing_keyboard()
            )
            await state.set_state(FormState.confirm_edit)
            return

        educations = snapshot_educations(snap)
        await state.clear()
        await state.update_data(sheet_snapshot=snap, education_options=educations)

        await message.answer(
            "🎓 Ta'lim shaklini tanlang:",
            reply_markup=paginated_keyboard("edu", educations, page=0, page_size=8, row_width=2)
        )
        await state.set_state(FormState.choosing_education)

    except Exception as e:
        logging.exception("start_handler: %s", e)
        await message.answer(f"❌ {str(e)}")


@dp.message(Command("recover"))
async def recover_handler(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "♻️ Tiklash rejimi.\n\n"
        "Eski ASOSIY raqamingizni kiriting.\n"
        "Format: +998XXXXXXXXX\n\n"
        "Bekor qilish uchun /cancel."
    )
    await state.set_state(FormState.recover_main)


@dp.message(Command("cancel"))
async def cancel_command(message: Message, state: FSMContext):
    await cancel_flow(state, message=message)


@dp.message(Command("help"))
async def help_handler(message: Message):
    await message.answer(
        "ℹ️ Buyruqlar:\n\n"
        "/start — ro'yxatdan o'tish yoki o'zgartirish\n"
        "/recover — boshqa akkauntga tiklash\n"
        "/cancel — bekor qilish\n"
        "/id — Telegram ID\n"
        "/ping — bot holati"
    )


@dp.message(Command("id"))
async def id_handler(message: Message):
    if not message.from_user:
        await message.answer("❌ ID aniqlanmadi.")
        return
    await message.answer(f"🆔 Sizning Telegram ID: {message.from_user.id}")


@dp.message(Command("ping"))
async def ping_handler(message: Message):
    await message.answer("🏓 Bot ishlayapti.")


# =========================
# CALLBACKS
# =========================
@dp.callback_query(F.data == "cancel")
async def cancel_callback(callback: CallbackQuery, state: FSMContext):
    await cancel_flow(state, callback=callback)


@dp.callback_query(F.data == "noop")
async def noop_handler(callback: CallbackQuery):
    await callback.answer()


@dp.callback_query(FormState.choosing_education, F.data.startswith("edu_page|"))
async def edu_page(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    items = data.get("education_options", [])
    page = int(callback.data.split("|")[1])
    await safe_edit(callback, "🎓 Ta'lim shaklini tanlang:", paginated_keyboard("edu", items, page=page, page_size=8, row_width=2))


@dp.callback_query(FormState.choosing_education, F.data.startswith("edu|"))
async def choose_education(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    items = data.get("education_options", [])
    idx = int(callback.data.split("|")[1])

    if idx < 0 or idx >= len(items):
        await callback.answer("Noto'g'ri tanlov", show_alert=True)
        return

    education = items[idx]
    snap = data["sheet_snapshot"]
    courses = snapshot_courses(snap, education)

    await state.update_data(education=education, course_options=courses)
    await state.set_state(FormState.choosing_course)

    await safe_edit(
        callback,
        f"🎓 Ta'lim shakli: {education}\n\n📚 Kursni tanlang:",
        paginated_keyboard("course", courses, page=0, page_size=9, row_width=3, back_cb="back_edu")
    )


@dp.callback_query(FormState.choosing_course, F.data == "back_edu")
async def back_edu(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    items = data.get("education_options", [])
    await state.set_state(FormState.choosing_education)
    await safe_edit(callback, "🎓 Ta'lim shaklini tanlang:", paginated_keyboard("edu", items, page=0, page_size=8, row_width=2))


@dp.callback_query(FormState.choosing_course, F.data.startswith("course_page|"))
async def course_page(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    items = data.get("course_options", [])
    page = int(callback.data.split("|")[1])
    education = data.get("education", "")
    await safe_edit(
        callback,
        f"🎓 Ta'lim shakli: {education}\n\n📚 Kursni tanlang:",
        paginated_keyboard("course", items, page=page, page_size=9, row_width=3, back_cb="back_edu")
    )


@dp.callback_query(FormState.choosing_course, F.data.startswith("course|"))
async def choose_course(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    items = data.get("course_options", [])
    idx = int(callback.data.split("|")[1])

    if idx < 0 or idx >= len(items):
        await callback.answer("Noto'g'ri tanlov", show_alert=True)
        return

    course = items[idx]
    education = data["education"]
    snap = data["sheet_snapshot"]
    groups = snapshot_groups(snap, education, course)

    await state.update_data(course=course, group_options=groups)
    await state.set_state(FormState.choosing_group)

    await safe_edit(
        callback,
        f"🎓 Ta'lim shakli: {education}\n📚 Kurs: {course}\n\n👥 Guruhni tanlang:",
        paginated_keyboard("group", groups, page=0, page_size=8, row_width=2, back_cb="back_course")
    )


@dp.callback_query(FormState.choosing_group, F.data == "back_course")
async def back_course(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    items = data.get("course_options", [])
    education = data.get("education", "")
    await state.set_state(FormState.choosing_course)
    await safe_edit(
        callback,
        f"🎓 Ta'lim shakli: {education}\n\n📚 Kursni tanlang:",
        paginated_keyboard("course", items, page=0, page_size=9, row_width=3, back_cb="back_edu")
    )


@dp.callback_query(FormState.choosing_group, F.data.startswith("group_page|"))
async def group_page(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    items = data.get("group_options", [])
    education = data.get("education", "")
    course = data.get("course", "")
    page = int(callback.data.split("|")[1])
    await safe_edit(
        callback,
        f"🎓 Ta'lim shakli: {education}\n📚 Kurs: {course}\n\n👥 Guruhni tanlang:",
        paginated_keyboard("group", items, page=page, page_size=8, row_width=2, back_cb="back_course")
    )


@dp.callback_query(FormState.choosing_group, F.data.startswith("group|"))
async def choose_group(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    items = data.get("group_options", [])
    idx = int(callback.data.split("|")[1])

    if idx < 0 or idx >= len(items):
        await callback.answer("Noto'g'ri tanlov", show_alert=True)
        return

    group = items[idx]
    education = data["education"]
    course = data["course"]
    snap = data["sheet_snapshot"]
    students = snapshot_students(snap, education, course, group)

    await state.update_data(group=group, student_options=students)
    await state.set_state(FormState.choosing_student)

    await safe_edit(
        callback,
        f"🎓 Ta'lim shakli: {education}\n📚 Kurs: {course}\n👥 Guruh: {group}\n\n🧑‍🎓 Talabani tanlang:",
        paginated_keyboard("student", students, page=0, page_size=8, row_width=1, back_cb="back_group")
    )


@dp.callback_query(FormState.choosing_student, F.data == "back_group")
async def back_group(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    items = data.get("group_options", [])
    education = data.get("education", "")
    course = data.get("course", "")
    await state.set_state(FormState.choosing_group)
    await safe_edit(
        callback,
        f"🎓 Ta'lim shakli: {education}\n📚 Kurs: {course}\n\n👥 Guruhni tanlang:",
        paginated_keyboard("group", items, page=0, page_size=8, row_width=2, back_cb="back_course")
    )


@dp.callback_query(FormState.choosing_student, F.data.startswith("student_page|"))
async def student_page(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    items = data.get("student_options", [])
    education = data.get("education", "")
    course = data.get("course", "")
    group = data.get("group", "")
    page = int(callback.data.split("|")[1])
    await safe_edit(
        callback,
        f"🎓 Ta'lim shakli: {education}\n📚 Kurs: {course}\n👥 Guruh: {group}\n\n🧑‍🎓 Talabani tanlang:",
        paginated_keyboard("student", items, page=page, page_size=8, row_width=1, back_cb="back_group")
    )


@dp.callback_query(FormState.choosing_student, F.data.startswith("student|"))
async def choose_student(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    items = data.get("student_options", [])
    idx = int(callback.data.split("|")[1])

    if idx < 0 or idx >= len(items):
        await callback.answer("Noto'g'ri tanlov", show_alert=True)
        return

    student = items[idx]
    snap = data["sheet_snapshot"]
    education = data["education"]
    course = data["course"]
    group = data["group"]

    if callback.from_user:
        existing = registration_by_tg_id(snap, str(callback.from_user.id))
        if existing:
            same = (
                existing["student"] == student
                and existing["course"] == course
                and existing["group"] == group
                and existing["education"] == education
            )
            if not same:
                await callback.answer("Bu Telegram ID boshqa talabaga bog'langan.", show_alert=True)
                return

    await state.update_data(student=student)
    await state.set_state(FormState.waiting_main_phone)

    await safe_edit(
        callback,
        f"🧑‍🎓 Talaba: {student}\n\n📱 Endi ASOSIY raqamni yozing\nFormat: +998XXXXXXXXX",
        None
    )


@dp.callback_query(FormState.confirm_edit, F.data == "edit_main")
async def edit_main(callback: CallbackQuery, state: FSMContext):
    await state.set_state(FormState.waiting_main_phone)
    await safe_edit(callback, "📱 Yangi ASOSIY raqamni yozing\nFormat: +998XXXXXXXXX", None)


@dp.callback_query(FormState.confirm_edit, F.data == "edit_extra")
async def edit_extra(callback: CallbackQuery, state: FSMContext):
    await state.set_state(FormState.waiting_extra_phone)
    await safe_edit(callback, "☎️ Yangi QO'SHIMCHA raqamni yozing\nFormat: +998XXXXXXXXX", None)


@dp.callback_query(FormState.confirm_edit, F.data == "edit_both")
async def edit_both(callback: CallbackQuery, state: FSMContext):
    await state.update_data(main_phone="", extra_phone="")
    await state.set_state(FormState.waiting_main_phone)
    await safe_edit(callback, "📱 Yangi ASOSIY raqamni yozing\nFormat: +998XXXXXXXXX", None)


@dp.callback_query(FormState.confirm_save, F.data == "save_rewrite")
async def rewrite_save(callback: CallbackQuery, state: FSMContext):
    await state.set_state(FormState.waiting_main_phone)
    await safe_edit(callback, "📱 Asosiy raqamni qayta kiriting\nFormat: +998XXXXXXXXX", None)


@dp.callback_query(FormState.confirm_save, F.data == "save_yes")
async def confirm_save(callback: CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()
        if not callback.from_user:
            await callback.answer("Foydalanuvchi aniqlanmadi", show_alert=True)
            return

        async with user_locks[callback.from_user.id]:
            await save_registration(
                student=data.get("student", ""),
                course=data.get("course", ""),
                group=data.get("group", ""),
                education=data.get("education", ""),
                main_phone=data.get("main_phone", ""),
                extra_phone=data.get("extra_phone", ""),
                telegram_id=str(callback.from_user.id),
                telegram_username=callback.from_user.username or "",
                telegram_full_name=callback.from_user.full_name or "",
            )

        await state.clear()
        await safe_edit(
            callback,
            "✅ Ma'lumot muvaffaqiyatli saqlandi\n\n"
            f"🎓 Ta'lim shakli: {data.get('education', '')}\n"
            f"📚 Kurs: {data.get('course', '')}\n"
            f"👥 Guruh: {data.get('group', '')}\n"
            f"🧑‍🎓 Talaba: {data.get('student', '')}\n"
            f"📱 Asosiy raqam: {data.get('main_phone', '')}\n"
            f"☎️ Qo'shimcha raqam: {data.get('extra_phone', '')}\n"
            f"🆔 Telegram ID: {callback.from_user.id}",
            None
        )

    except Exception as e:
        logging.exception("confirm_save: %s", e)
        if callback.message:
            await callback.message.answer(f"❌ {str(e)}")
        await callback.answer()


# =========================
# TEXT INPUTS
# =========================
@dp.message(FormState.waiting_main_phone, F.text)
async def input_main_phone(message: Message, state: FSMContext):
    text = norm(message.text)
    if text == "/cancel":
        await cancel_flow(state, message=message)
        return

    phone = norm_phone(text)
    if not valid_phone(phone):
        await message.answer("⚠️ Asosiy raqam noto'g'ri. Masalan: +998901234567")
        return

    await state.update_data(main_phone=phone)
    await state.set_state(FormState.waiting_extra_phone)
    await message.answer("☎️ Endi QO'SHIMCHA raqamni yozing\nFormat: +998XXXXXXXXX")


@dp.message(FormState.waiting_extra_phone, F.text)
async def input_extra_phone(message: Message, state: FSMContext):
    text = norm(message.text)
    if text == "/cancel":
        await cancel_flow(state, message=message)
        return

    phone = norm_phone(text)
    if not valid_phone(phone):
        await message.answer("⚠️ Qo'shimcha raqam noto'g'ri. Masalan: +998901234567")
        return

    data = await state.get_data()
    main_phone = data.get("main_phone", "")
    if not main_phone:
        await message.answer("⚠️ Asosiy raqam topilmadi. Qaytadan /start bosing.")
        await state.clear()
        return

    if phone == main_phone:
        await message.answer("⚠️ Qo'shimcha raqam asosiy raqam bilan bir xil bo'lmasin.")
        return

    await state.update_data(extra_phone=phone)
    await show_confirm(message, state)


@dp.message(FormState.recover_main, F.text)
async def recover_main_input(message: Message, state: FSMContext):
    text = norm(message.text)
    if text == "/cancel":
        await cancel_flow(state, message=message)
        return

    phone = norm_phone(text)
    if not valid_phone(phone):
        await message.answer("⚠️ Asosiy raqam noto'g'ri. Masalan: +998901234567")
        return

    await state.update_data(recover_main_phone=phone)
    await state.set_state(FormState.recover_extra)
    await message.answer("☎️ Endi eski QO'SHIMCHA raqamni kiriting\nFormat: +998XXXXXXXXX")


@dp.message(FormState.recover_extra, F.text)
async def recover_extra_input(message: Message, state: FSMContext):
    text = norm(message.text)
    if text == "/cancel":
        await cancel_flow(state, message=message)
        return

    phone = norm_phone(text)
    if not valid_phone(phone):
        await message.answer("⚠️ Qo'shimcha raqam noto'g'ri. Masalan: +998901234567")
        return

    data = await state.get_data()
    main_phone = data.get("recover_main_phone", "")
    if phone == main_phone:
        await message.answer("⚠️ Qo'shimcha raqam asosiy raqam bilan bir xil bo'lmasin.")
        return

    snap = await fetch_snapshot()
    found = registration_by_phones(snap, main_phone, phone)
    if not found:
        await message.answer("❌ Bunday raqamlar juftligi topilmadi.")
        return

    if not message.from_user:
        await message.answer("❌ Foydalanuvchi aniqlanmadi.")
        return

    async with user_locks[message.from_user.id]:
        await rebind_account(
            main_phone=main_phone,
            extra_phone=phone,
            telegram_id=str(message.from_user.id),
            username=message.from_user.username or "",
            full_name=message.from_user.full_name or "",
        )

    await state.clear()
    await message.answer(
        "✅ Akkaunt muvaffaqiyatli tiklandi.\n\n"
        f"🎓 Ta'lim shakli: {found['education']}\n"
        f"📚 Kurs: {found['course']}\n"
        f"👥 Guruh: {found['group']}\n"
        f"🧑‍🎓 Talaba: {found['student']}\n"
        f"📱 Asosiy raqam: {found['main_phone']}\n"
        f"☎️ Qo'shimcha raqam: {found['extra_phone']}\n"
        f"🆔 Yangi Telegram ID: {message.from_user.id}"
    )


# =========================
# ADMIN
# =========================
@dp.message(Command("admin"))
async def admin_handler(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        await message.answer("⛔ Siz admin emassiz.")
        return
    await message.answer("🛠 Admin buyruqlari:\n\n/stats\n/find <matn>\n/refresh")


@dp.message(Command("stats"))
async def admin_stats(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    try:
        total_students, total_groups, total_educations = await get_sheet_stats()
        await message.answer(
            "📊 Jadval statistikasi\n\n"
            f"🧑‍🎓 Talabalar soni: {total_students}\n"
            f"👥 Guruhlar soni: {total_groups}\n"
            f"🎓 Ta'lim shakllari soni: {total_educations}"
        )
    except Exception as e:
        await message.answer(f"❌ {str(e)}")


@dp.message(Command("refresh"))
async def admin_refresh(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    try:
        _ = await fetch_snapshot()
        await message.answer("✅ Jadval tekshirildi.")
    except Exception as e:
        await message.answer(f"❌ {str(e)}")


@dp.message(Command("find"))
async def admin_find(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    parts = norm(message.text).split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("🔎 Qidiruv uchun: /find talaba_yoki_guruh")
        return

    try:
        results = await search_students(parts[1])
        if not results:
            await message.answer("ℹ️ Hech narsa topilmadi.")
            return

        chunks = []
        for item in results:
            chunks.append(
                f"🧑‍🎓 {item['student']}\n"
                f"👥 {item['group']} | 📚 {item['course']} | 🎓 {item['education']}\n"
                f"📱 {item['main_phone'] or '-'}\n"
                f"☎️ {item['extra_phone'] or '-'}\n"
                f"🆔 {item['telegram_id'] or '-'}\n"
                f"🧾 Yuborish soni: {item['count'] or '0'}"
            )
        await message.answer("🔎 Qidiruv natijalari\n\n" + "\n\n".join(chunks))
    except Exception as e:
        await message.answer(f"❌ {str(e)}")


@dp.message()
async def fallback_handler(message: Message):
    await message.answer("ℹ️ Qayta boshlash uchun /start bosing.")


async def get_sheet_stats() -> Tuple[int, int, int]:
    snap = await fetch_snapshot()
    records = snap["records"]
    cols = snap["columns"]

    total_students = len(records)
    total_groups = len(set(
        (norm(r.get(cols["education"], "")), norm(r.get(cols["course"], "")), norm(r.get(cols["group"], "")))
        for r in records
    ))
    total_educations = len(set(norm(r.get(cols["education"], "")) for r in records if norm(r.get(cols["education"], ""))))
    return total_students, total_groups, total_educations


async def search_students(keyword: str) -> List[dict]:
    snap = await fetch_snapshot()
    records = snap["records"]
    cols = snap["columns"]
    keyword = norm(keyword).lower()

    found = []
    for row in records:
        student = norm(row.get(cols["student"], ""))
        group = norm(row.get(cols["group"], ""))
        course = norm(row.get(cols["course"], ""))
        education = norm(row.get(cols["education"], ""))

        if keyword in student.lower() or keyword in group.lower():
            found.append({
                "student": student,
                "group": group,
                "course": course,
                "education": education,
                "main_phone": norm(row.get("Asosiy nomer", "")),
                "extra_phone": norm(row.get("Qo'shimcha nomer", "")),
                "telegram_id": norm(row.get("Telegram ID", "")),
                "count": norm(row.get("Yuborish soni", "")),
            })
    return found[:10]


async def main():
    logging.info("Bot ishga tushdi...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
