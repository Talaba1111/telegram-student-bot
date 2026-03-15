import asyncio
import json
import logging
import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import gspread
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatMemberStatus
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup, ReplyKeyboardRemove
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
    confirm_edit = State()
    waiting_main_phone = State()
    waiting_extra_phone = State()
    confirm_save = State()
    admin_search = State()


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


def make_keyboard(items: List[str], per_row: int = 2, add_cancel: bool = True) -> ReplyKeyboardMarkup:
    keyboard = []
    row = []

    for item in items:
        row.append(KeyboardButton(text=item))
        if len(row) == per_row:
            keyboard.append(row)
            row = []

    if row:
        keyboard.append(row)

    if add_cancel:
        keyboard.append([KeyboardButton(text="❌ Bekor qilish")])

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        one_time_keyboard=True
    )


def confirm_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Tasdiqlash"), KeyboardButton(text="✏️ Qayta kiritish")],
            [KeyboardButton(text="❌ Bekor qilish")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )


def edit_choice_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Asosiy raqamni o'zgartirish")],
            [KeyboardButton(text="☎️ Qo'shimcha raqamni o'zgartirish")],
            [KeyboardButton(text="🔁 Ikkalasini qayta kiritish")],
            [KeyboardButton(text="❌ Bekor qilish")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )


def admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Statistika"), KeyboardButton(text="🔄 Yangilash")],
            [KeyboardButton(text="🔎 Qidiruv")],
            [KeyboardButton(text="🏠 Chiqish")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )


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
    try:
        return await asyncio.to_thread(get_worksheet_sync)
    except Exception as e:
        raise ValueError(f"Google Sheets ulanishida xatolik: {e}")


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
    education_col = await get_column_name(
        worksheet,
        ["Ta'lim shakli", "Ta’lim shakli", "Talim shakli", "talim shakli"]
    )
    course_col = await get_column_name(worksheet, ["Kurs", "kurs"])
    group_col = await get_column_name(worksheet, ["Guruh", "guruh", "Group", "group"])
    student_col = await get_column_name(
        worksheet,
        ["F.I.SH.", "F.I.SH", "FISH", "Fish", "FIO", "Talaba"]
    )

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

    return {
        "records": records,
        "columns": columns,
    }


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

    return {
        "main_phone": "",
        "extra_phone": "",
        "telegram_id": "",
    }


async def find_student_row_index(
    worksheet,
    student: str,
    course: str,
    group: str,
    education: str
) -> Optional[int]:
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

    row_index = await find_student_row_index(
        worksheet=worksheet,
        student=student,
        course=course,
        group=group,
        education=education,
    )

    if not row_index:
        raise ValueError("Tanlangan talaba jadvaldan topilmadi")

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

    updates = [
        (row_index, main_col, main_phone),
        (row_index, extra_col, extra_phone),
        (row_index, tg_id_col, telegram_id),
        (row_index, tg_username_col, telegram_username),
        (row_index, tg_name_col, telegram_full_name),
        (row_index, updated_col, now_str),
        (row_index, count_col, str(current_count + 1)),
    ]

    for r, c, value in updates:
        if c:
            await asyncio.to_thread(worksheet.update_cell, r, c, value)


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


async def show_education_step(message: Message, state: FSMContext):
    snapshot = await fetch_sheet_snapshot()
    educations = get_educations_from_snapshot(snapshot)

    if not educations:
        await message.answer("⚠️ Ta'lim shakllari topilmadi")
        return

    await state.clear()
    await state.update_data(sheet_snapshot=snapshot)

    await message.answer(
        "🎓 Ta'lim shaklini tanlang:",
        reply_markup=make_keyboard(educations, per_row=2)
    )
    await state.set_state(RegisterStates.choosing_education)


async def show_confirm_step(message: Message, state: FSMContext):
    data = await state.get_data()

    await message.answer(
        "📝 Kiritilgan ma'lumotlarni tekshiring:\n\n"
        f"🎓 Ta'lim shakli: {data.get('education', '')}\n"
        f"📚 Kurs: {data.get('course', '')}\n"
        f"👥 Guruh: {data.get('group', '')}\n"
        f"🧑‍🎓 Talaba: {data.get('student', '')}\n"
        f"📱 Asosiy raqam: {data.get('main_phone', '')}\n"
        f"☎️ Qo'shimcha raqam: {data.get('extra_phone', '')}\n\n"
        "✅ Tasdiqlaysizmi yoki ✏️ qayta kiritasizmi?",
        reply_markup=confirm_keyboard()
    )
    await state.set_state(RegisterStates.confirm_save)


async def cancel_flow(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "❌ Amal bekor qilindi.\nQayta boshlash uchun /start bosing.",
        reply_markup=ReplyKeyboardRemove()
    )


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

        await show_education_step(message, state)

    except Exception as e:
        logging.exception("start_handler xatolik: %s", e)
        await message.answer(f"❌ {str(e)}")


@dp.message(Command("help"))
async def help_handler(message: Message):
    await message.answer(
        "ℹ️ Foydalanish tartibi:\n\n"
        "1. /start bosing\n"
        "2. 🎓 Ta'lim shaklini tanlang\n"
        "3. 📚 Kursni tanlang\n"
        "4. 👥 Guruhni tanlang\n"
        "5. 🧑‍🎓 Talabani tanlang\n"
        "6. 📱 Asosiy raqamni yozing\n"
        "7. ☎️ Qo'shimcha raqamni yozing\n"
        "8. ✅ Tasdiqlang\n\n"
        "Telefon raqamlar faqat +998XXXXXXXXX formatida qabul qilinadi."
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


@dp.message(F.text == "❌ Bekor qilish")
async def cancel_handler(message: Message, state: FSMContext):
    await cancel_flow(message, state)


@dp.message(RegisterStates.choosing_education, F.text)
async def education_handler(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        snapshot = data.get("sheet_snapshot")

        if not snapshot:
            await message.answer("⚠️ Sessiya tugagan. Qayta /start bosing.")
            await state.clear()
            return

        education = normalize_text(message.text)
        educations = get_educations_from_snapshot(snapshot)

        if education not in educations:
            await message.answer("⚠️ Iltimos, tugmalardan birini tanlang.")
            return

        courses = get_courses_from_snapshot(snapshot, education)
        if not courses:
            await message.answer("⚠️ Bu ta'lim shakli uchun kurs topilmadi.")
            return

        await state.update_data(education=education)
        await message.answer(
            "📚 Kursni tanlang:",
            reply_markup=make_keyboard(courses, per_row=3)
        )
        await state.set_state(RegisterStates.choosing_course)

    except Exception as e:
        logging.exception("education_handler xatolik: %s", e)
        await message.answer(f"❌ {str(e)}")


@dp.message(RegisterStates.choosing_course, F.text)
async def course_handler(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        snapshot = data.get("sheet_snapshot")
        education = data.get("education", "")

        if not snapshot:
            await message.answer("⚠️ Sessiya tugagan. Qayta /start bosing.")
            await state.clear()
            return

        course = normalize_text(message.text)
        courses = get_courses_from_snapshot(snapshot, education)

        if course not in courses:
            await message.answer("⚠️ Iltimos, kursni tugmalardan tanlang.")
            return

        groups = get_groups_from_snapshot(snapshot, education, course)
        if not groups:
            await message.answer("⚠️ Bu kurs uchun guruh topilmadi.")
            return

        await state.update_data(course=course)
        await message.answer(
            "👥 Guruhni tanlang:",
            reply_markup=make_keyboard(groups, per_row=2)
        )
        await state.set_state(RegisterStates.choosing_group)

    except Exception as e:
        logging.exception("course_handler xatolik: %s", e)
        await message.answer(f"❌ {str(e)}")


@dp.message(RegisterStates.choosing_group, F.text)
async def group_handler(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        snapshot = data.get("sheet_snapshot")
        education = data.get("education", "")
        course = data.get("course", "")

        if not snapshot:
            await message.answer("⚠️ Sessiya tugagan. Qayta /start bosing.")
            await state.clear()
            return

        group = normalize_text(message.text)
        groups = get_groups_from_snapshot(snapshot, education, course)

        if group not in groups:
            await message.answer("⚠️ Iltimos, guruhni tugmalardan tanlang.")
            return

        students = get_students_from_snapshot(snapshot, education, course, group)
        if not students:
            await message.answer("⚠️ Bu guruhda talabalar topilmadi.")
            return

        await state.update_data(group=group)
        await message.answer(
            "🧑‍🎓 Talabani tanlang:",
            reply_markup=make_keyboard(students, per_row=1)
        )
        await state.set_state(RegisterStates.choosing_student)

    except Exception as e:
        logging.exception("group_handler xatolik: %s", e)
        await message.answer(f"❌ {str(e)}")


@dp.message(RegisterStates.choosing_student, F.text)
async def student_handler(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        snapshot = data.get("sheet_snapshot")
        education = data.get("education", "")
        course = data.get("course", "")
        group = data.get("group", "")

        if not snapshot:
            await message.answer("⚠️ Sessiya tugagan. Qayta /start bosing.")
            await state.clear()
            return

        student = normalize_text(message.text)
        students = get_students_from_snapshot(snapshot, education, course, group)

        if student not in students:
            await message.answer("⚠️ Iltimos, talabani tugmalardan tanlang.")
            return

        saved = get_saved_data_from_snapshot(snapshot, student, course, group, education)
        await state.update_data(student=student)

        if saved["telegram_id"] and message.from_user and saved["telegram_id"] == str(message.from_user.id):
            if saved["main_phone"] or saved["extra_phone"]:
                await message.answer(
                    "ℹ️ Bu talaba uchun oldin ma'lumot saqlangan.\n\n"
                    f"📱 Asosiy raqam: {saved['main_phone'] or '-'}\n"
                    f"☎️ Qo'shimcha raqam: {saved['extra_phone'] or '-'}\n\n"
                    "Qaysi qismini o'zgartirmoqchisiz?",
                    reply_markup=edit_choice_keyboard()
                )
                await state.set_state(RegisterStates.confirm_edit)
                return

        await message.answer(
            f"✅ Tanlangan talaba: {student}\n\n"
            "📱 Endi ASOSIY raqamni yozing\n"
            "Format: +998XXXXXXXXX",
            reply_markup=make_keyboard([], add_cancel=True)
        )
        await state.set_state(RegisterStates.waiting_main_phone)

    except Exception as e:
        logging.exception("student_handler xatolik: %s", e)
        await message.answer(f"❌ {str(e)}")


@dp.message(RegisterStates.confirm_edit, F.text)
async def confirm_edit_handler(message: Message, state: FSMContext):
    try:
        text = normalize_text(message.text)

        if text == "📱 Asosiy raqamni o'zgartirish":
            await message.answer(
                "📱 Yangi ASOSIY raqamni yozing\nFormat: +998XXXXXXXXX",
                reply_markup=make_keyboard([], add_cancel=True)
            )
            await state.set_state(RegisterStates.waiting_main_phone)
            return

        if text == "☎️ Qo'shimcha raqamni o'zgartirish":
            await message.answer(
                "☎️ Yangi QO'SHIMCHA raqamni yozing\nFormat: +998XXXXXXXXX",
                reply_markup=make_keyboard([], add_cancel=True)
            )
            await state.set_state(RegisterStates.waiting_extra_phone)
            return

        if text == "🔁 Ikkalasini qayta kiritish":
            await state.update_data(main_phone="", extra_phone="")
            await message.answer(
                "📱 Yangi ASOSIY raqamni yozing\nFormat: +998XXXXXXXXX",
                reply_markup=make_keyboard([], add_cancel=True)
            )
            await state.set_state(RegisterStates.waiting_main_phone)
            return

        await message.answer("⚠️ Iltimos, tugmalardan birini tanlang.")

    except Exception as e:
        logging.exception("confirm_edit_handler xatolik: %s", e)
        await message.answer(f"❌ {str(e)}")


@dp.message(RegisterStates.waiting_main_phone, F.text)
async def main_phone_text_handler(message: Message, state: FSMContext):
    try:
        text = normalize_text(message.text)

        if text == "❌ Bekor qilish":
            await cancel_flow(message, state)
            return

        phone = normalize_phone(text)
        if not is_valid_uz_phone(phone):
            await message.answer("⚠️ Asosiy raqam noto'g'ri. Masalan: +998901234567")
            return

        await state.update_data(main_phone=phone)

        await message.answer(
            "☎️ Endi QO'SHIMCHA raqamni yozing\nFormat: +998XXXXXXXXX",
            reply_markup=make_keyboard([], add_cancel=True)
        )
        await state.set_state(RegisterStates.waiting_extra_phone)

    except Exception as e:
        logging.exception("main_phone_text_handler xatolik: %s", e)
        await message.answer(f"❌ {str(e)}")


@dp.message(RegisterStates.waiting_extra_phone, F.text)
async def extra_phone_text_handler(message: Message, state: FSMContext):
    try:
        text = normalize_text(message.text)

        if text == "❌ Bekor qilish":
            await cancel_flow(message, state)
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

    except Exception as e:
        logging.exception("extra_phone_text_handler xatolik: %s", e)
        await message.answer(f"❌ {str(e)}")


@dp.message(RegisterStates.confirm_save, F.text)
async def confirm_save_handler(message: Message, state: FSMContext):
    try:
        text = normalize_text(message.text)
        data = await state.get_data()

        if text == "✏️ Qayta kiritish":
            await message.answer(
                "📱 Asosiy raqamni qayta kiriting\nFormat: +998XXXXXXXXX",
                reply_markup=make_keyboard([], add_cancel=True)
            )
            await state.set_state(RegisterStates.waiting_main_phone)
            return

        if text != "✅ Tasdiqlash":
            await message.answer("⚠️ Iltimos, tugmalardan birini tanlang.")
            return

        if not message.from_user:
            await message.answer("❌ Foydalanuvchi aniqlanmadi.")
            return

        user_id = message.from_user.id

        async with user_locks[user_id]:
            await save_full_data_to_sheet(
                student=data.get("student", ""),
                course=data.get("course", ""),
                group=data.get("group", ""),
                education=data.get("education", ""),
                main_phone=data.get("main_phone", ""),
                extra_phone=data.get("extra_phone", ""),
                telegram_id=str(message.from_user.id),
                telegram_username=message.from_user.username or "",
                telegram_full_name=message.from_user.full_name or "",
            )

        await message.answer(
            "✅ Ma'lumot muvaffaqiyatli saqlandi\n\n"
            f"🎓 Ta'lim shakli: {data.get('education', '')}\n"
            f"📚 Kurs: {data.get('course', '')}\n"
            f"👥 Guruh: {data.get('group', '')}\n"
            f"🧑‍🎓 Talaba: {data.get('student', '')}\n"
            f"📱 Asosiy raqam: {data.get('main_phone', '')}\n"
            f"☎️ Qo'shimcha raqam: {data.get('extra_phone', '')}\n"
            f"🆔 Telegram ID: {message.from_user.id}",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.clear()

    except Exception as e:
        logging.exception("confirm_save_handler xatolik: %s", e)
        await message.answer(f"❌ {str(e)}")


@dp.message(Command("admin"))
async def admin_handler(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        await message.answer("⛔ Siz admin emassiz.")
        return

    await state.clear()
    await message.answer(
        "🛠 Admin panel\nKerakli bo'limni tanlang:",
        reply_markup=admin_keyboard()
    )


@dp.message(F.text == "📊 Statistika")
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


@dp.message(F.text == "🔄 Yangilash")
async def admin_refresh_handler(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    try:
        _ = await fetch_sheet_snapshot()
        await message.answer("✅ Jadval tekshirildi. Yangi talabalar va o'chirilganlar darrov qabul qilinadi.")
    except Exception as e:
        await message.answer(f"❌ {str(e)}")


@dp.message(F.text == "🔎 Qidiruv")
async def admin_search_start_handler(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    await state.set_state(RegisterStates.admin_search)
    await message.answer(
        "🔎 Qidiruv uchun talaba F.I.SH. yoki guruh yozing:",
        reply_markup=make_keyboard([], add_cancel=True)
    )


@dp.message(RegisterStates.admin_search, F.text)
async def admin_search_handler(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    text = normalize_text(message.text)
    if text == "❌ Bekor qilish":
        await cancel_flow(message, state)
        return

    try:
        results = await search_students(text)
        if not results:
            await message.answer("ℹ️ Hech narsa topilmadi.", reply_markup=admin_keyboard())
            await state.clear()
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

        await message.answer(
            "🔎 Qidiruv natijalari\n\n" + "\n\n".join(chunks),
            reply_markup=admin_keyboard()
        )
        await state.clear()
    except Exception as e:
        await message.answer(f"❌ {str(e)}")


@dp.message(F.text == "🏠 Chiqish")
async def admin_exit_handler(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    await state.clear()
    await message.answer(
        "✅ Admin paneldan chiqildi.",
        reply_markup=ReplyKeyboardRemove()
    )


@dp.message()
async def fallback_handler(message: Message):
    await message.answer("ℹ️ Qayta boshlash uchun /start bosing.")


async def main():
    logging.info("Bot ishga tushdi...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
