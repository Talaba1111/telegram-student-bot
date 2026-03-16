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
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message, ReplyKeyboardRemove
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

ADMIN_IDS = {
    int(x.strip()) for x in ADMIN_IDS_RAW.split(",")
    if x.strip().isdigit()
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

user_locks = defaultdict(asyncio.Lock)


class RegisterStates(StatesGroup):
    choosing_education = State()
    choosing_course = State()
    choosing_group = State()
    choosing_student = State()
    waiting_main_phone = State()
    waiting_extra_phone = State()
    confirm_save = State()
    confirm_edit = State()
    recover_main_phone = State()
    recover_extra_phone = State()
    admin_search = State()


# =========================
# HELPERS
# =========================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def normalize_text(value) -> str:
    return str(value).strip() if value is not None else ""


def normalize_header(text: str) -> str:
    return (
        str(text)
        .strip()
        .lower()
        .replace("’", "'")
        .replace("`", "'")
        .replace("ʻ", "'")
    )


def normalize_phone(phone: str) -> str:
    phone = str(phone).strip()
    phone = re.sub(r"[^\d+]", "", phone)
    if phone.startswith("998") and not phone.startswith("+"):
        phone = "+" + phone
    return phone


def is_valid_uz_phone(phone: str) -> bool:
    return bool(re.fullmatch(r"^\+998\d{9}$", phone))


def unique_values(values: List[str]) -> List[str]:
    result = []
    for value in values:
        v = normalize_text(value)
        if v and v not in result:
            result.append(v)
    return result


def build_paginated_keyboard(
    prefix: str,
    items: List[str],
    page: int = 0,
    page_size: int = 8,
    row_width: int = 1,
    back_callback: Optional[str] = None,
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

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"{prefix}_page|{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"{prefix}_page|{page+1}"))
    if nav:
        rows.append(nav)

    bottom = []
    if back_callback:
        bottom.append(InlineKeyboardButton(text="🔙 Orqaga", callback_data=back_callback))
    bottom.append(InlineKeyboardButton(text="❌ Bekor qilish", callback_data="cancel"))
    rows.append(bottom)

    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Tasdiqlash", callback_data="save_yes"),
                InlineKeyboardButton(text="✏️ Qayta kiritish", callback_data="save_rewrite"),
            ],
            [InlineKeyboardButton(text="❌ Bekor qilish", callback_data="cancel")],
        ]
    )


def build_existing_edit_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📱 Asosiy raqamni o'zgartirish", callback_data="edit_main")],
            [InlineKeyboardButton(text="☎️ Qo'shimcha raqamni o'zgartirish", callback_data="edit_extra")],
            [InlineKeyboardButton(text="🔁 Ikkalasini qayta kiritish", callback_data="edit_both")],
            [InlineKeyboardButton(text="❌ Bekor qilish", callback_data="cancel")],
        ]
    )


# =========================
# CHANNEL CHECK
# =========================
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


# =========================
# GOOGLE SHEETS
# =========================
def get_credentials():
    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    except Exception as e:
        raise ValueError(f"GOOGLE_CREDENTIALS_JSON noto'g'ri formatda: {e}")

    if "private_key" in creds_dict:
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_info(creds_dict, scopes=scopes)


def get_worksheet_sync():
    credentials = get_credentials()
    client = gspread.authorize(credentials)
    spreadsheet = client.open(GOOGLE_SHEET_NAME)
    return spreadsheet.sheet1


async def get_worksheet():
    return await asyncio.to_thread(get_worksheet_sync)


async def get_headers(worksheet) -> List[str]:
    return await asyncio.to_thread(lambda: [str(h).strip() for h in worksheet.row_values(1)])


async def get_all_records(worksheet):
    return await asyncio.to_thread(worksheet.get_all_records)


async def get_column_name(worksheet, possible_names: List[str]) -> Optional[str]:
    headers = await get_headers(worksheet)
    normalized_headers = {normalize_header(h): h for h in headers}
    for name in possible_names:
        key = normalize_header(name)
        if key in normalized_headers:
            return normalized_headers[key]
    return None


async def get_col_index_by_name(worksheet, col_name: str) -> Optional[int]:
    headers = await get_headers(worksheet)
    for idx, header in enumerate(headers, start=1):
        if normalize_header(header) == normalize_header(col_name):
            return idx
    return None


async def ensure_extra_columns(worksheet):
    headers = await get_headers(worksheet)
    columns_to_add = [
        "Asosiy nomer",
        "Qo'shimcha nomer",
        "Telegram ID",
        "Telegram Username",
        "Telegram Full Name",
        "Oxirgi yangilanish",
        "Yuborish soni",
    ]
    current_headers = headers[:]
    for col in columns_to_add:
        if col not in current_headers:
            await asyncio.to_thread(worksheet.update_cell, 1, len(current_headers) + 1, col)
            current_headers.append(col)


async def get_required_columns(worksheet) -> Dict[str, str]:
    education_col = await get_column_name(worksheet, ["Ta'lim shakli", "Ta’lim shakli", "Talim shakli", "talim shakli"])
    course_col = await get_column_name(worksheet, ["Kurs", "kurs"])
    group_col = await get_column_name(worksheet, ["Guruh", "guruh", "Group", "group"])
    student_col = await get_column_name(worksheet, ["F.I.SH.", "F.I.SH", "FISH", "Fish", "FIO", "Talaba"])

    if not education_col:
        raise ValueError("Google Sheetsda Ta'lim shakli ustuni topilmadi")
    if not course_col:
        raise ValueError("Google Sheetsda Kurs ustuni topilmadi")
    if not group_col:
        raise ValueError("Google Sheetsda Guruh ustuni topilmadi")
    if not student_col:
        raise ValueError("Google Sheetsda F.I.SH. ustuni topilmadi")

    return {
        "education": education_col,
        "course": course_col,
        "group": group_col,
        "student": student_col,
    }


async def fetch_sheet_snapshot() -> Dict:
    worksheet = await get_worksheet()
    await ensure_extra_columns(worksheet)
    records = await get_all_records(worksheet)
    columns = await get_required_columns(worksheet)
    return {"records": records, "columns": columns}


def get_educations_from_snapshot(snapshot: Dict) -> List[str]:
    records = snapshot["records"]
    columns = snapshot["columns"]
    return unique_values([normalize_text(r.get(columns["education"], "")) for r in records])


def get_courses_from_snapshot(snapshot: Dict, education: str) -> List[str]:
    records = snapshot["records"]
    columns = snapshot["columns"]
    return unique_values([
        normalize_text(r.get(columns["course"], ""))
        for r in records
        if normalize_text(r.get(columns["education"], "")) == education
    ])


def get_groups_from_snapshot(snapshot: Dict, education: str, course: str) -> List[str]:
    records = snapshot["records"]
    columns = snapshot["columns"]
    return unique_values([
        normalize_text(r.get(columns["group"], ""))
        for r in records
        if normalize_text(r.get(columns["education"], "")) == education
        and normalize_text(r.get(columns["course"], "")) == course
    ])


def get_students_from_snapshot(snapshot: Dict, education: str, course: str, group: str) -> List[str]:
    records = snapshot["records"]
    columns = snapshot["columns"]
    return unique_values([
        normalize_text(r.get(columns["student"], ""))
        for r in records
        if normalize_text(r.get(columns["education"], "")) == education
        and normalize_text(r.get(columns["course"], "")) == course
        and normalize_text(r.get(columns["group"], "")) == group
    ])


def get_registration_by_tg_id(snapshot: Dict, tg_id: str) -> Optional[Dict[str, str]]:
    records = snapshot["records"]
    columns = snapshot["columns"]
    for row in records:
        row_tg_id = normalize_text(row.get("Telegram ID", ""))
        if row_tg_id == tg_id:
            return {
                "student": normalize_text(row.get(columns["student"], "")),
                "course": normalize_text(row.get(columns["course"], "")),
                "group": normalize_text(row.get(columns["group"], "")),
                "education": normalize_text(row.get(columns["education"], "")),
                "main_phone": normalize_text(row.get("Asosiy nomer", "")),
                "extra_phone": normalize_text(row.get("Qo'shimcha nomer", "")),
                "telegram_id": row_tg_id,
            }
    return None


def find_student_by_phones(snapshot: Dict, main_phone: str, extra_phone: str) -> Optional[Dict[str, str]]:
    records = snapshot["records"]
    columns = snapshot["columns"]
    for row in records:
        if (
            normalize_text(row.get("Asosiy nomer", "")) == main_phone
            and normalize_text(row.get("Qo'shimcha nomer", "")) == extra_phone
        ):
            return {
                "student": normalize_text(row.get(columns["student"], "")),
                "course": normalize_text(row.get(columns["course"], "")),
                "group": normalize_text(row.get(columns["group"], "")),
                "education": normalize_text(row.get(columns["education"], "")),
                "main_phone": normalize_text(row.get("Asosiy nomer", "")),
                "extra_phone": normalize_text(row.get("Qo'shimcha nomer", "")),
                "telegram_id": normalize_text(row.get("Telegram ID", "")),
            }
    return None


def get_saved_data_from_snapshot(snapshot: Dict, student: str, course: str, group: str, education: str) -> Dict[str, str]:
    records = snapshot["records"]
    columns = snapshot["columns"]
    for row in records:
        if (
            normalize_text(row.get(columns["student"], "")) == student
            and normalize_text(row.get(columns["course"], "")) == course
            and normalize_text(row.get(columns["group"], "")) == group
            and normalize_text(row.get(columns["education"], "")) == education
        ):
            return {
                "main_phone": normalize_text(row.get("Asosiy nomer", "")),
                "extra_phone": normalize_text(row.get("Qo'shimcha nomer", "")),
                "telegram_id": normalize_text(row.get("Telegram ID", "")),
            }
    return {"main_phone": "", "extra_phone": "", "telegram_id": ""}


async def find_student_row_index(worksheet, student: str, course: str, group: str, education: str) -> Optional[int]:
    records = await get_all_records(worksheet)
    columns = await get_required_columns(worksheet)
    for row_index, row in enumerate(records, start=2):
        if (
            normalize_text(row.get(columns["student"], "")) == student
            and normalize_text(row.get(columns["course"], "")) == course
            and normalize_text(row.get(columns["group"], "")) == group
            and normalize_text(row.get(columns["education"], "")) == education
        ):
            return row_index
    return None


async def rebind_telegram_owner(main_phone: str, extra_phone: str, telegram_id: str, username: str, full_name: str):
    worksheet = await get_worksheet()
    await ensure_extra_columns(worksheet)
    records = await get_all_records(worksheet)
    columns = await get_required_columns(worksheet)

    target_index = None
    for row_index, row in enumerate(records, start=2):
        if (
            normalize_text(row.get("Asosiy nomer", "")) == main_phone
            and normalize_text(row.get("Qo'shimcha nomer", "")) == extra_phone
        ):
            target_index = row_index
            break

    if not target_index:
        raise ValueError("Bunday raqamlar juftligi topilmadi")

    # Yangi id boshqa talabada turmasin
    for row_index, row in enumerate(records, start=2):
        row_tg_id = normalize_text(row.get("Telegram ID", ""))
        if row_tg_id == telegram_id and row_index != target_index:
            student_name = normalize_text(row.get(columns["student"], ""))
            raise ValueError(f"Bu Telegram ID allaqachon boshqa talabaga biriktirilgan: {student_name}")

    tg_id_col = await get_col_index_by_name(worksheet, "Telegram ID")
    tg_username_col = await get_col_index_by_name(worksheet, "Telegram Username")
    tg_name_col = await get_col_index_by_name(worksheet, "Telegram Full Name")
    updated_col = await get_col_index_by_name(worksheet, "Oxirgi yangilanish")

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cells = []

    if tg_id_col:
        cells.append(Cell(target_index, tg_id_col, telegram_id))
    if tg_username_col:
        cells.append(Cell(target_index, tg_username_col, username))
    if tg_name_col:
        cells.append(Cell(target_index, tg_name_col, full_name))
    if updated_col:
        cells.append(Cell(target_index, updated_col, now_str))

    if cells:
        await asyncio.to_thread(worksheet.update_cells, cells)


async def save_full_data_to_sheet(
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
    worksheet = await get_worksheet()
    await ensure_extra_columns(worksheet)

    row_index = await find_student_row_index(worksheet, student, course, group, education)
    if not row_index:
        raise ValueError("Tanlangan talaba jadvaldan topilmadi")

    records = await get_all_records(worksheet)
    columns = await get_required_columns(worksheet)

    # Bir Telegram ID boshqa talabada ishlatilmasin
    for idx, row in enumerate(records, start=2):
        row_tg_id = normalize_text(row.get("Telegram ID", ""))
        if row_tg_id == telegram_id and idx != row_index:
            student_name = normalize_text(row.get(columns["student"], ""))
            raise ValueError(f"Bu Telegram ID allaqachon boshqa talabaga biriktirilgan: {student_name}")

    main_col = await get_col_index_by_name(worksheet, "Asosiy nomer")
    extra_col = await get_col_index_by_name(worksheet, "Qo'shimcha nomer")
    tg_id_col = await get_col_index_by_name(worksheet, "Telegram ID")
    tg_username_col = await get_col_index_by_name(worksheet, "Telegram Username")
    tg_name_col = await get_col_index_by_name(worksheet, "Telegram Full Name")
    updated_col = await get_col_index_by_name(worksheet, "Oxirgi yangilanish")
    count_col = await get_col_index_by_name(worksheet, "Yuborish soni")

    existing_main = ""
    existing_extra = ""
    current_count = 0

    if main_col:
        existing_main = normalize_text((await asyncio.to_thread(worksheet.cell, row_index, main_col)).value)
    if extra_col:
        existing_extra = normalize_text((await asyncio.to_thread(worksheet.cell, row_index, extra_col)).value)
    if count_col:
        cell_value = (await asyncio.to_thread(worksheet.cell, row_index, count_col)).value
        if cell_value:
            try:
                current_count = int(str(cell_value).strip())
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
    if tg_username_col:
        cells.append(Cell(row_index, tg_username_col, telegram_username))
    if tg_name_col:
        cells.append(Cell(row_index, tg_name_col, telegram_full_name))
    if updated_col:
        cells.append(Cell(row_index, updated_col, now_str))
    if count_col:
        cells.append(Cell(row_index, count_col, str(current_count + 1)))

    if cells:
        await asyncio.to_thread(worksheet.update_cells, cells)


async def get_sheet_stats() -> Tuple[int, int, int]:
    snapshot = await fetch_sheet_snapshot()
    records = snapshot["records"]
    columns = snapshot["columns"]

    total_students = len(records)
    total_groups = len(set(
        (normalize_text(r.get(columns["education"], "")),
         normalize_text(r.get(columns["course"], "")),
         normalize_text(r.get(columns["group"], "")))
        for r in records
    ))
    total_educations = len(set(
        normalize_text(r.get(columns["education"], ""))
        for r in records if normalize_text(r.get(columns["education"], ""))
    ))
    return total_students, total_groups, total_educations


async def search_students(keyword: str) -> List[dict]:
    snapshot = await fetch_sheet_snapshot()
    records = snapshot["records"]
    columns = snapshot["columns"]

    keyword = normalize_text(keyword).lower()
    found = []

    for row in records:
        student = normalize_text(row.get(columns["student"], ""))
        group = normalize_text(row.get(columns["group"], ""))
        course = normalize_text(row.get(columns["course"], ""))
        education = normalize_text(row.get(columns["education"], ""))

        if keyword in student.lower() or keyword in group.lower():
            found.append({
                "student": student,
                "group": group,
                "course": course,
                "education": education,
                "main_phone": normalize_text(row.get("Asosiy nomer", "")),
                "extra_phone": normalize_text(row.get("Qo'shimcha nomer", "")),
                "telegram_id": normalize_text(row.get("Telegram ID", "")),
                "count": normalize_text(row.get("Yuborish soni", "")),
            })

    return found[:10]


# =========================
# FLOW HELPERS
# =========================
async def cancel_flow(state: FSMContext, target_message: Optional[Message] = None, callback: Optional[CallbackQuery] = None):
    await state.clear()
    text = "❌ Amal bekor qilindi.\nQayta boshlash uchun /start bosing."

    if callback and callback.message:
        await callback.message.edit_text(text)
        await callback.answer()
    elif target_message:
        await target_message.answer(text, reply_markup=ReplyKeyboardRemove())


async def ask_educations(message: Message, state: FSMContext):
    snapshot = await fetch_sheet_snapshot()
    educations = get_educations_from_snapshot(snapshot)

    if not educations:
        await message.answer("⚠️ Ta'lim shakllari topilmadi")
        return

    await state.clear()
    await state.update_data(sheet_snapshot=snapshot, education_options=educations)

    await message.answer(
        "🎓 Ta'lim shaklini tanlang:",
        reply_markup=build_paginated_keyboard("edu", educations, page=0, page_size=8, row_width=2)
    )
    await state.set_state(RegisterStates.choosing_education)


async def show_confirm_step(message: Message, state: FSMContext):
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
    await message.answer(text, reply_markup=build_confirm_keyboard())
    await state.set_state(RegisterStates.confirm_save)


# =========================
# USER COMMANDS
# =========================
@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    try:
        if not message.from_user:
            await message.answer("❌ Foydalanuvchi aniqlanmadi.")
            return

        subscribed = await check_subscription(message.from_user.id)
        if not subscribed:
            await message.answer(
                "📢 Botdan foydalanish uchun avval kanalga obuna bo‘ling.\n\n"
                f"🔗 Kanal: {REQUIRED_CHANNEL}\n\n"
                "✅ Obuna bo‘lgach, qayta /start bosing."
            )
            return

        snapshot = await fetch_sheet_snapshot()
        existing = get_registration_by_tg_id(snapshot, str(message.from_user.id))

        if existing:
            await state.clear()
            await state.update_data(
                sheet_snapshot=snapshot,
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
                "Agar akkauntingiz o'chgan bo'lsa, /recover ishlating.",
                reply_markup=ReplyKeyboardRemove()
            )
            await message.answer("Tanlang:", reply_markup=build_existing_edit_keyboard())
            await state.set_state(RegisterStates.confirm_edit)
            return

        await ask_educations(message, state)

    except Exception as e:
        logging.exception("start_handler xatolik: %s", e)
        await message.answer(f"❌ {str(e)}")


@dp.message(Command("recover"))
async def recover_start_handler(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "♻️ Tiklash rejimi.\n\n"
        "Avval eski ASOSIY raqamingizni kiriting.\n"
        "Format: +998XXXXXXXXX\n\n"
        "Bekor qilish uchun /cancel."
    )
    await state.set_state(RegisterStates.recover_main_phone)


@dp.message(Command("cancel"))
async def cancel_command_handler(message: Message, state: FSMContext):
    await cancel_flow(state, target_message=message)


@dp.message(Command("help"))
async def help_handler(message: Message):
    await message.answer(
        "ℹ️ Foydalanish tartibi:\n\n"
        "/start — ro'yxatdan o'tish yoki tahrirlash\n"
        "/recover — udalenniy akkauntdan keyin yangi akkauntga tiklash\n"
        "/cancel — bekor qilish\n"
        "/id — Telegram ID ni ko'rish\n\n"
        "Telefon raqam faqat +998XXXXXXXXX formatida qabul qilinadi."
    )


@dp.message(Command("id"))
async def my_id_handler(message: Message):
    if not message.from_user:
        await message.answer("❌ ID aniqlanmadi.")
        return
    await message.answer(f"🆔 Sizning Telegram ID: {message.from_user.id}")


@dp.message(Command("ping"))
async def ping_handler(message: Message):
    await message.answer("🏓 Bot ishlayapti.")


# =========================
# CALLBACK HANDLERS
# =========================
@dp.callback_query(F.data == "cancel")
async def callback_cancel_handler(callback: CallbackQuery, state: FSMContext):
    await cancel_flow(state, callback=callback)


@dp.callback_query(F.data == "noop")
async def noop_callback(callback: CallbackQuery):
    await callback.answer()


@dp.callback_query(RegisterStates.choosing_education, F.data.startswith("edu_page|"))
async def edu_page_callback(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    options = data.get("education_options", [])
    page = int(callback.data.split("|")[1])
    if callback.message:
        await callback.message.edit_reply_markup(
            reply_markup=build_paginated_keyboard("edu", options, page=page, page_size=8, row_width=2)
        )
    await callback.answer()


@dp.callback_query(RegisterStates.choosing_education, F.data.startswith("edu|"))
async def education_callback(callback: CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()
        options = data.get("education_options", [])
        idx = int(callback.data.split("|")[1])

        if idx < 0 or idx >= len(options):
            await callback.answer("Noto'g'ri tanlov", show_alert=True)
            return

        education = options[idx]
        snapshot = data.get("sheet_snapshot")
        courses = get_courses_from_snapshot(snapshot, education)

        if not courses:
            await callback.answer("Kurs topilmadi", show_alert=True)
            return

        await state.update_data(education=education, course_options=courses)

        if callback.message:
            await callback.message.edit_text(
                f"🎓 Ta'lim shakli: {education}\n\n📚 Kursni tanlang:"
            )
            await callback.message.edit_reply_markup(
                reply_markup=build_paginated_keyboard("course", courses, page=0, page_size=9, row_width=3)
            )
        await state.set_state(RegisterStates.choosing_course)
        await callback.answer()

    except Exception as e:
        logging.exception("education_callback xatolik: %s", e)
        if callback.message:
            await callback.message.edit_text(f"❌ {str(e)}")
        await callback.answer()


@dp.callback_query(RegisterStates.choosing_course, F.data.startswith("course_page|"))
async def course_page_callback(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    options = data.get("course_options", [])
    page = int(callback.data.split("|")[1])
    if callback.message:
        await callback.message.edit_reply_markup(
            reply_markup=build_paginated_keyboard(
                "course", options, page=page, page_size=9, row_width=3, back_callback="back_to_edu"
            )
        )
    await callback.answer()


@dp.callback_query(RegisterStates.choosing_course, F.data == "back_to_edu")
async def back_to_edu_callback(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    options = data.get("education_options", [])
    if callback.message:
        await callback.message.edit_text("🎓 Ta'lim shaklini tanlang:")
        await callback.message.edit_reply_markup(
            reply_markup=build_paginated_keyboard("edu", options, page=0, page_size=8, row_width=2)
        )
    await state.set_state(RegisterStates.choosing_education)
    await callback.answer()


@dp.callback_query(RegisterStates.choosing_course, F.data.startswith("course|"))
async def course_callback(callback: CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()
        options = data.get("course_options", [])
        idx = int(callback.data.split("|")[1])

        if idx < 0 or idx >= len(options):
            await callback.answer("Noto'g'ri tanlov", show_alert=True)
            return

        course = options[idx]
        snapshot = data.get("sheet_snapshot")
        education = data.get("education", "")
        groups = get_groups_from_snapshot(snapshot, education, course)

        if not groups:
            await callback.answer("Guruh topilmadi", show_alert=True)
            return

        await state.update_data(course=course, group_options=groups)

        if callback.message:
            await callback.message.edit_text(
                f"🎓 Ta'lim shakli: {education}\n📚 Kurs: {course}\n\n👥 Guruhni tanlang:"
            )
            await callback.message.edit_reply_markup(
                reply_markup=build_paginated_keyboard("group", groups, page=0, page_size=8, row_width=2, back_callback="back_to_course")
            )
        await state.set_state(RegisterStates.choosing_group)
        await callback.answer()

    except Exception as e:
        logging.exception("course_callback xatolik: %s", e)
        if callback.message:
            await callback.message.edit_text(f"❌ {str(e)}")
        await callback.answer()


@dp.callback_query(RegisterStates.choosing_group, F.data.startswith("group_page|"))
async def group_page_callback(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    options = data.get("group_options", [])
    page = int(callback.data.split("|")[1])
    if callback.message:
        await callback.message.edit_reply_markup(
            reply_markup=build_paginated_keyboard("group", options, page=page, page_size=8, row_width=2, back_callback="back_to_course")
        )
    await callback.answer()


@dp.callback_query(RegisterStates.choosing_group, F.data == "back_to_course")
async def back_to_course_callback(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    options = data.get("course_options", [])
    education = data.get("education", "")
    if callback.message:
        await callback.message.edit_text(
            f"🎓 Ta'lim shakli: {education}\n\n📚 Kursni tanlang:"
        )
        await callback.message.edit_reply_markup(
            reply_markup=build_paginated_keyboard("course", options, page=0, page_size=9, row_width=3, back_callback="back_to_edu")
        )
    await state.set_state(RegisterStates.choosing_course)
    await callback.answer()


@dp.callback_query(RegisterStates.choosing_group, F.data.startswith("group|"))
async def group_callback(callback: CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()
        options = data.get("group_options", [])
        idx = int(callback.data.split("|")[1])

        if idx < 0 or idx >= len(options):
            await callback.answer("Noto'g'ri tanlov", show_alert=True)
            return

        group = options[idx]
        snapshot = data.get("sheet_snapshot")
        education = data.get("education", "")
        course = data.get("course", "")
        students = get_students_from_snapshot(snapshot, education, course, group)

        if not students:
            await callback.answer("Talaba topilmadi", show_alert=True)
            return

        await state.update_data(group=group, student_options=students)

        if callback.message:
            await callback.message.edit_text(
                f"🎓 Ta'lim shakli: {education}\n📚 Kurs: {course}\n👥 Guruh: {group}\n\n🧑‍🎓 Talabani tanlang:"
            )
            await callback.message.edit_reply_markup(
                reply_markup=build_paginated_keyboard("student", students, page=0, page_size=8, row_width=1, back_callback="back_to_group")
            )
        await state.set_state(RegisterStates.choosing_student)
        await callback.answer()

    except Exception as e:
        logging.exception("group_callback xatolik: %s", e)
        if callback.message:
            await callback.message.edit_text(f"❌ {str(e)}")
        await callback.answer()


@dp.callback_query(RegisterStates.choosing_student, F.data.startswith("student_page|"))
async def student_page_callback(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    options = data.get("student_options", [])
    page = int(callback.data.split("|")[1])
    if callback.message:
        await callback.message.edit_reply_markup(
            reply_markup=build_paginated_keyboard("student", options, page=page, page_size=8, row_width=1, back_callback="back_to_group")
        )
    await callback.answer()


@dp.callback_query(RegisterStates.choosing_student, F.data == "back_to_group")
async def back_to_group_callback(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    options = data.get("group_options", [])
    education = data.get("education", "")
    course = data.get("course", "")
    if callback.message:
        await callback.message.edit_text(
            f"🎓 Ta'lim shakli: {education}\n📚 Kurs: {course}\n\n👥 Guruhni tanlang:"
        )
        await callback.message.edit_reply_markup(
            reply_markup=build_paginated_keyboard("group", options, page=0, page_size=8, row_width=2, back_callback="back_to_course")
        )
    await state.set_state(RegisterStates.choosing_group)
    await callback.answer()


@dp.callback_query(RegisterStates.choosing_student, F.data.startswith("student|"))
async def student_callback(callback: CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()
        options = data.get("student_options", [])
        idx = int(callback.data.split("|")[1])

        if idx < 0 or idx >= len(options):
            await callback.answer("Noto'g'ri tanlov", show_alert=True)
            return

        student = options[idx]
        snapshot = data.get("sheet_snapshot")
        education = data.get("education", "")
        course = data.get("course", "")
        group = data.get("group", "")

        if callback.from_user:
            existing = get_registration_by_tg_id(snapshot, str(callback.from_user.id))
            if existing:
                same_row = (
                    existing["student"] == student
                    and existing["course"] == course
                    and existing["group"] == group
                    and existing["education"] == education
                )
                if not same_row:
                    await callback.answer("Bu Telegram ID boshqa talabaga bog'langan.", show_alert=True)
                    return

        await state.update_data(student=student)

        if callback.message:
            await callback.message.edit_text(
                f"🧑‍🎓 Talaba: {student}\n\n📱 Endi ASOSIY raqamni yozing\nFormat: +998XXXXXXXXX"
            )
            await callback.message.edit_reply_markup(reply_markup=None)

        await state.set_state(RegisterStates.waiting_main_phone)
        await callback.answer()

    except Exception as e:
        logging.exception("student_callback xatolik: %s", e)
        if callback.message:
            await callback.message.edit_text(f"❌ {str(e)}")
        await callback.answer()


@dp.callback_query(RegisterStates.confirm_edit, F.data == "edit_main")
async def edit_main_callback(callback: CallbackQuery, state: FSMContext):
    if callback.message:
        await callback.message.edit_text("📱 Yangi ASOSIY raqamni yozing\nFormat: +998XXXXXXXXX")
        await callback.message.edit_reply_markup(reply_markup=None)
    await state.set_state(RegisterStates.waiting_main_phone)
    await callback.answer()


@dp.callback_query(RegisterStates.confirm_edit, F.data == "edit_extra")
async def edit_extra_callback(callback: CallbackQuery, state: FSMContext):
    if callback.message:
        await callback.message.edit_text("☎️ Yangi QO'SHIMCHA raqamni yozing\nFormat: +998XXXXXXXXX")
        await callback.message.edit_reply_markup(reply_markup=None)
    await state.set_state(RegisterStates.waiting_extra_phone)
    await callback.answer()


@dp.callback_query(RegisterStates.confirm_edit, F.data == "edit_both")
async def edit_both_callback(callback: CallbackQuery, state: FSMContext):
    await state.update_data(main_phone="", extra_phone="")
    if callback.message:
        await callback.message.edit_text("📱 Yangi ASOSIY raqamni yozing\nFormat: +998XXXXXXXXX")
        await callback.message.edit_reply_markup(reply_markup=None)
    await state.set_state(RegisterStates.waiting_main_phone)
    await callback.answer()


# =========================
# TEXT INPUTS
# =========================
@dp.message(RegisterStates.waiting_main_phone, F.text)
async def main_phone_text_handler(message: Message, state: FSMContext):
    text = normalize_text(message.text)

    if text == "/cancel":
        await cancel_flow(state, target_message=message)
        return

    phone = normalize_phone(text)
    if not is_valid_uz_phone(phone):
        await message.answer("⚠️ Asosiy raqam noto'g'ri. Masalan: +998901234567")
        return

    await state.update_data(main_phone=phone)
    await message.answer("☎️ Endi QO'SHIMCHA raqamni yozing\nFormat: +998XXXXXXXXX")
    await state.set_state(RegisterStates.waiting_extra_phone)


@dp.message(RegisterStates.waiting_extra_phone, F.text)
async def extra_phone_text_handler(message: Message, state: FSMContext):
    text = normalize_text(message.text)

    if text == "/cancel":
        await cancel_flow(state, target_message=message)
        return

    extra_phone = normalize_phone(text)
    if not is_valid_uz_phone(extra_phone):
        await message.answer("⚠️ Qo'shimcha raqam noto'g'ri. Masalan: +998901234567")
        return

    data = await state.get_data()
    main_phone = data.get("main_phone", "")

    if not main_phone:
        await message.answer("⚠️ Asosiy raqam topilmadi. Qaytadan /start bosing.")
        await state.clear()
        return

    if extra_phone == main_phone:
        await message.answer("⚠️ Qo'shimcha raqam asosiy raqam bilan bir xil bo'lmasin.")
        return

    await state.update_data(extra_phone=extra_phone)
    await show_confirm_step(message, state)


@dp.callback_query(RegisterStates.confirm_save, F.data == "save_rewrite")
async def rewrite_callback(callback: CallbackQuery, state: FSMContext):
    if callback.message:
        await callback.message.edit_text("📱 Asosiy raqamni qayta kiriting\nFormat: +998XXXXXXXXX")
        await callback.message.edit_reply_markup(reply_markup=None)
    await state.set_state(RegisterStates.waiting_main_phone)
    await callback.answer()


@dp.callback_query(RegisterStates.confirm_save, F.data == "save_yes")
async def confirm_save_callback(callback: CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()

        if not callback.from_user:
            await callback.answer("Foydalanuvchi aniqlanmadi", show_alert=True)
            return

        user_id = callback.from_user.id

        async with user_locks[user_id]:
            await save_full_data_to_sheet(
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

        if callback.message:
            await callback.message.edit_text(
                "✅ Ma'lumot muvaffaqiyatli saqlandi\n\n"
                f"🎓 Ta'lim shakli: {data.get('education', '')}\n"
                f"📚 Kurs: {data.get('course', '')}\n"
                f"👥 Guruh: {data.get('group', '')}\n"
                f"🧑‍🎓 Talaba: {data.get('student', '')}\n"
                f"📱 Asosiy raqam: {data.get('main_phone', '')}\n"
                f"☎️ Qo'shimcha raqam: {data.get('extra_phone', '')}\n"
                f"🆔 Telegram ID: {callback.from_user.id}"
            )
        await callback.answer("Saqlandi")

    except Exception as e:
        logging.exception("confirm_save_callback xatolik: %s", e)
        if callback.message:
            await callback.message.answer(f"❌ {str(e)}")
        await callback.answer()


# =========================
# RECOVERY FLOW
# =========================
@dp.message(RegisterStates.recover_main_phone, F.text)
async def recover_main_phone_handler(message: Message, state: FSMContext):
    text = normalize_text(message.text)

    if text == "/cancel":
        await cancel_flow(state, target_message=message)
        return

    phone = normalize_phone(text)
    if not is_valid_uz_phone(phone):
        await message.answer("⚠️ Asosiy raqam noto'g'ri. Masalan: +998901234567")
        return

    await state.update_data(recover_main_phone=phone)
    await message.answer("☎️ Endi eski QO'SHIMCHA raqamni kiriting\nFormat: +998XXXXXXXXX")
    await state.set_state(RegisterStates.recover_extra_phone)


@dp.message(RegisterStates.recover_extra_phone, F.text)
async def recover_extra_phone_handler(message: Message, state: FSMContext):
    text = normalize_text(message.text)

    if text == "/cancel":
        await cancel_flow(state, target_message=message)
        return

    extra_phone = normalize_phone(text)
    if not is_valid_uz_phone(extra_phone):
        await message.answer("⚠️ Qo'shimcha raqam noto'g'ri. Masalan: +998901234567")
        return

    data = await state.get_data()
    main_phone = data.get("recover_main_phone", "")

    if extra_phone == main_phone:
        await message.answer("⚠️ Qo'shimcha raqam asosiy raqam bilan bir xil bo'lmasin.")
        return

    snapshot = await fetch_sheet_snapshot()
    found = find_student_by_phones(snapshot, main_phone, extra_phone)

    if not found:
        await message.answer("❌ Bunday raqamlar juftligi topilmadi.")
        return

    if not message.from_user:
        await message.answer("❌ Foydalanuvchi aniqlanmadi.")
        return

    async with user_locks[message.from_user.id]:
        await rebind_telegram_owner(
            main_phone=main_phone,
            extra_phone=extra_phone,
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

    await message.answer(
        "🛠 Admin buyruqlari:\n\n"
        "/stats - statistika\n"
        "/find <matn> - qidiruv\n"
        "/refresh - jadvalni tekshirish"
    )


@dp.message(Command("stats"))
async def admin_stats_handler(message: Message):
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
async def admin_refresh_handler(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    try:
        _ = await fetch_sheet_snapshot()
        await message.answer("✅ Jadval tekshirildi.")
    except Exception as e:
        await message.answer(f"❌ {str(e)}")


@dp.message(Command("find"))
async def admin_find_handler(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    text = normalize_text(message.text)
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("🔎 Qidiruv uchun: /find talaba_yoki_guruh")
        return

    query = parts[1]
    try:
        results = await search_students(query)
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


async def main():
    logging.info("Bot ishga tushdi...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
