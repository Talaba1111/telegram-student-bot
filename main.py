import asyncio
import html
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

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
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
    admin_search = State()
    admin_search_by_id = State()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def normalize_text(value) -> str:
    return str(value).strip() if value is not None else ""


def safe_html(text: str) -> str:
    return html.escape(normalize_text(text))


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


def unique_sorted(values: List[str]) -> List[str]:
    result = []
    for value in values:
        value = normalize_text(value)
        if value and value not in result:
            result.append(value)
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


def contact_keyboard(label: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=label, request_contact=True)],
            [KeyboardButton(text="❌ Bekor qilish")]
        ],
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
            [KeyboardButton(text="🔎 Qidiruv"), KeyboardButton(text="🆔 ID bo'yicha qidiruv")],
            [KeyboardButton(text="🕒 Oxirgi yangilanishlar"), KeyboardButton(text="📋 Ustunlar")],
            [KeyboardButton(text="🧾 Loglar"), KeyboardButton(text="🏠 Chiqish")]
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
        raise ValueError("Google Sheetsda <b>Ta'lim shakli</b> ustuni topilmadi")
    if not course_col:
        raise ValueError("Google Sheetsda <b>Kurs</b> ustuni topilmadi")
    if not group_col:
        raise ValueError("Google Sheetsda <b>Guruh</b> ustuni topilmadi")
    if not student_col:
        raise ValueError("Google Sheetsda <b>F.I.SH.</b> ustuni topilmadi")

    return {
        "education": education_col,
        "course": course_col,
        "group": group_col,
        "student": student_col,
    }


async def get_educations() -> List[str]:
    worksheet = await get_worksheet()
    records = await get_all_records(worksheet)
    columns = await get_required_columns(worksheet)
    return unique_sorted([normalize_text(r.get(columns["education"], "")) for r in records])


async def get_courses(education: str) -> List[str]:
    worksheet = await get_worksheet()
    records = await get_all_records(worksheet)
    columns = await get_required_columns(worksheet)

    return unique_sorted([
        normalize_text(r.get(columns["course"], ""))
        for r in records
        if normalize_text(r.get(columns["education"], "")) == education
    ])


async def get_groups(education: str, course: str) -> List[str]:
    worksheet = await get_worksheet()
    records = await get_all_records(worksheet)
    columns = await get_required_columns(worksheet)

    return unique_sorted([
        normalize_text(r.get(columns["group"], ""))
        for r in records
        if normalize_text(r.get(columns["education"], "")) == education
        and normalize_text(r.get(columns["course"], "")) == course
    ])


async def get_students(education: str, course: str, group: str) -> List[str]:
    worksheet = await get_worksheet()
    records = await get_all_records(worksheet)
    columns = await get_required_columns(worksheet)

    return unique_sorted([
        normalize_text(r.get(columns["student"], ""))
        for r in records
        if normalize_text(r.get(columns["education"], "")) == education
        and normalize_text(r.get(columns["course"], "")) == course
        and normalize_text(r.get(columns["group"], "")) == group
    ])


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


async def get_student_saved_data(
    student: str,
    course: str,
    group: str,
    education: str,
) -> Dict[str, str]:
    worksheet = await get_worksheet()
    await ensure_extra_columns(worksheet)
    row_index = await find_student_row_index(worksheet, student, course, group, education)

    if not row_index:
        raise ValueError("Talaba topilmadi")

    main_col = await get_col_index_by_name(worksheet, "Asosiy nomer")
    extra_col = await get_col_index_by_name(worksheet, "Qo'shimcha nomer")
    tg_id_col = await get_col_index_by_name(worksheet, "Telegram ID")

    main_phone = ""
    extra_phone = ""
    tg_id = ""

    if main_col:
        main_phone = normalize_text((await asyncio.to_thread(worksheet.cell, row_index, main_col)).value)
    if extra_col:
        extra_phone = normalize_text((await asyncio.to_thread(worksheet.cell, row_index, extra_col)).value)
    if tg_id_col:
        tg_id = normalize_text((await asyncio.to_thread(worksheet.cell, row_index, tg_id_col)).value)

    return {
        "main_phone": main_phone,
        "extra_phone": extra_phone,
        "telegram_id": tg_id,
        "row_index": str(row_index),
    }


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
    worksheet = await get_worksheet()
    records = await get_all_records(worksheet)
    columns = await get_required_columns(worksheet)

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
    worksheet = await get_worksheet()
    records = await get_all_records(worksheet)
    columns = await get_required_columns(worksheet)

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


async def get_recent_updates(limit: int = 10) -> List[dict]:
    worksheet = await get_worksheet()
    records = await get_all_records(worksheet)

    result = []
    for row in records:
        updated = normalize_text(row.get("Oxirgi yangilanish", ""))
        if updated:
            student_value = normalize_text(row.get("F.I.SH.", "")) or normalize_text(row.get("F.I.SH", ""))
            group_value = normalize_text(row.get("Guruh", ""))
            tg_id_value = normalize_text(row.get("Telegram ID", ""))

            result.append({
                "student": student_value,
                "group": group_value,
                "updated": updated,
                "telegram_id": tg_id_value,
            })

    result.sort(key=lambda x: x["updated"], reverse=True)
    return result[:limit]


async def cancel_flow(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "❌ <b>Amal bekor qilindi</b>\nQayta boshlash uchun /start bosing.",
        reply_markup=ReplyKeyboardRemove()
    )


async def show_education_step(message: Message, state: FSMContext):
    educations = await get_educations()

    if not educations:
        await message.answer("⚠️ <b>Ta'lim shakllari topilmadi</b>")
        return

    await state.clear()
    await message.answer(
        "🎓 <b>Ta'lim shaklini tanlang:</b>",
        reply_markup=make_keyboard(educations, per_row=2)
    )
    await state.set_state(RegisterStates.choosing_education)


async def show_confirm_step(message: Message, state: FSMContext):
    data = await state.get_data()

    education_value = safe_html(data.get("education", ""))
    course_value = safe_html(data.get("course", ""))
    group_value = safe_html(data.get("group", ""))
    student_value = safe_html(data.get("student", ""))
    main_phone_value = safe_html(data.get("main_phone", ""))
    extra_phone_value = safe_html(data.get("extra_phone", ""))

    await message.answer(
        "📝 <b>Kiritilgan ma'lumotlarni tekshiring:</b>\n\n"
        f"🎓 Ta'lim shakli: <b>{education_value}</b>\n"
        f"📚 Kurs: <b>{course_value}</b>\n"
        f"👥 Guruh: <b>{group_value}</b>\n"
        f"🧑‍🎓 Talaba: <b>{student_value}</b>\n"
        f"📱 Asosiy raqam: <b>{main_phone_value}</b>\n"
        f"☎️ Qo'shimcha raqam: <b>{extra_phone_value}</b>\n\n"
        "✅ Tasdiqlaysizmi yoki ✏️ qayta kiritasizmi?",
        reply_markup=confirm_keyboard()
    )
    await state.set_state(RegisterStates.confirm_save)


@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    try:
        if not message.from_user:
            await message.answer("❌ Foydalanuvchi aniqlanmadi.")
            return

        subscribed = await check_subscription(message.from_user.id)
        if not subscribed:
            channel_value = safe_html(REQUIRED_CHANNEL)
            await message.answer(
                "📢 <b>Botdan foydalanish uchun avval kanalga obuna bo‘ling.</b>\n\n"
                f"🔗 Kanal: <b>{channel_value}</b>\n\n"
                "✅ Obuna bo‘lgach, qayta /start bosing."
            )
            return

        await show_education_step(message, state)

    except Exception as e:
        logging.exception("start_handler xatolik: %s", e)
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(Command("help"))
async def help_handler(message: Message):
    await message.answer(
        "ℹ️ <b>Foydalanish tartibi</b>\n\n"
        "1. /start bosing\n"
        "2. 🎓 Ta'lim shaklini tanlang\n"
        "3. 📚 Kursni tanlang\n"
        "4. 👥 Guruhni tanlang\n"
        "5. 🧑‍🎓 Talabani tanlang\n"
        "6. 📱 Asosiy raqamni kiriting\n"
        "7. ☎️ Qo'shimcha raqamni kiriting\n"
        "8. ✅ Tasdiqlang\n\n"
        "Telefon raqamlar faqat <b>+998XXXXXXXXX</b> formatida qabul qilinadi."
    )


@dp.message(Command("id"))
async def my_id_handler(message: Message):
    if not message.from_user:
        await message.answer("❌ ID aniqlanmadi.")
        return

    await message.answer(f"🆔 <b>Sizning Telegram ID:</b> <code>{message.from_user.id}</code>")


@dp.message(Command("ping"))
async def ping_handler(message: Message):
    await message.answer("🏓 Bot ishlayapti.")


@dp.message(F.text == "❌ Bekor qilish")
async def cancel_handler(message: Message, state: FSMContext):
    await cancel_flow(message, state)


@dp.message(RegisterStates.choosing_education, F.text)
async def education_handler(message: Message, state: FSMContext):
    try:
        education = normalize_text(message.text)
        educations = await get_educations()

        if education not in educations:
            await message.answer("⚠️ Iltimos, tugmalardan birini tanlang.")
            return

        courses = await get_courses(education)
        if not courses:
            await message.answer("⚠️ Bu ta'lim shakli uchun kurs topilmadi.")
            return

        await state.update_data(education=education)
        await message.answer(
            "📚 <b>Kursni tanlang:</b>",
            reply_markup=make_keyboard(courses, per_row=3)
        )
        await state.set_state(RegisterStates.choosing_course)

    except Exception as e:
        logging.exception("education_handler xatolik: %s", e)
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(RegisterStates.choosing_course, F.text)
async def course_handler(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        education = data.get("education", "")
        course = normalize_text(message.text)

        courses = await get_courses(education)
        if course not in courses:
            await message.answer("⚠️ Iltimos, kursni tugmalardan tanlang.")
            return

        groups = await get_groups(education, course)
        if not groups:
            await message.answer("⚠️ Bu kurs uchun guruh topilmadi.")
            return

        await state.update_data(course=course)
        await message.answer(
            "👥 <b>Guruhni tanlang:</b>",
            reply_markup=make_keyboard(groups, per_row=2)
        )
        await state.set_state(RegisterStates.choosing_group)

    except Exception as e:
        logging.exception("course_handler xatolik: %s", e)
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(RegisterStates.choosing_group, F.text)
async def group_handler(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        education = data.get("education", "")
        course = data.get("course", "")
        group = normalize_text(message.text)

        groups = await get_groups(education, course)
        if group not in groups:
            await message.answer("⚠️ Iltimos, guruhni tugmalardan tanlang.")
            return

        students = await get_students(education, course, group)
        if not students:
            await message.answer("⚠️ Bu guruhda talabalar topilmadi.")
            return

        await state.update_data(group=group)
        await message.answer(
            "🧑‍🎓 <b>Talabani tanlang:</b>",
            reply_markup=make_keyboard(students, per_row=1)
        )
        await state.set_state(RegisterStates.choosing_student)

    except Exception as e:
        logging.exception("group_handler xatolik: %s", e)
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(RegisterStates.choosing_student, F.text)
async def student_handler(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        education = data.get("education", "")
        course = data.get("course", "")
        group = data.get("group", "")
        student = normalize_text(message.text)

        students = await get_students(education, course, group)
        if student not in students:
            await message.answer("⚠️ Iltimos, talabani tugmalardan tanlang.")
            return

        saved = await get_student_saved_data(student, course, group, education)

        await state.update_data(student=student)

        if saved["telegram_id"] and message.from_user and saved["telegram_id"] == str(message.from_user.id):
            if saved["main_phone"] or saved["extra_phone"]:
                main_value = safe_html(saved["main_phone"] or "-")
                extra_value = safe_html(saved["extra_phone"] or "-")

                await message.answer(
                    "ℹ️ <b>Bu talaba uchun oldin ma'lumot saqlangan.</b>\n\n"
                    f"📱 Asosiy raqam: <b>{main_value}</b>\n"
                    f"☎️ Qo'shimcha raqam: <b>{extra_value}</b>\n\n"
                    "Qaysi qismini o'zgartirmoqchisiz?",
                    reply_markup=edit_choice_keyboard()
                )
                await state.set_state(RegisterStates.confirm_edit)
                return

        student_value = safe_html(student)
        await message.answer(
            f"✅ <b>Tanlangan talaba:</b> {student_value}\n\n"
            "📱 <b>Endi ASOSIY raqamni yuboring</b>\n"
            "Format: <b>+998XXXXXXXXX</b>",
            reply_markup=contact_keyboard("📲 Asosiy raqamni kontakt qilib yuborish")
        )
        await state.set_state(RegisterStates.waiting_main_phone)

    except Exception as e:
        logging.exception("student_handler xatolik: %s", e)
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(RegisterStates.confirm_edit, F.text)
async def confirm_edit_handler(message: Message, state: FSMContext):
    try:
        text = normalize_text(message.text)

        if text == "📱 Asosiy raqamni o'zgartirish":
            await message.answer(
                "📱 <b>Yangi ASOSIY raqamni yuboring</b>\nFormat: <b>+998XXXXXXXXX</b>",
                reply_markup=contact_keyboard("📲 Asosiy raqamni kontakt qilib yuborish")
            )
            await state.set_state(RegisterStates.waiting_main_phone)
            return

        if text == "☎️ Qo'shimcha raqamni o'zgartirish":
            await message.answer(
                "☎️ <b>Yangi QO'SHIMCHA raqamni yuboring</b>\nFormat: <b>+998XXXXXXXXX</b>",
                reply_markup=contact_keyboard("📲 Qo'shimcha raqamni kontakt qilib yuborish")
            )
            await state.set_state(RegisterStates.waiting_extra_phone)
            return

        if text == "🔁 Ikkalasini qayta kiritish":
            await state.update_data(main_phone="", extra_phone="")
            await message.answer(
                "📱 <b>Yangi ASOSIY raqamni yuboring</b>\nFormat: <b>+998XXXXXXXXX</b>",
                reply_markup=contact_keyboard("📲 Asosiy raqamni kontakt qilib yuborish")
            )
            await state.set_state(RegisterStates.waiting_main_phone)
            return

        await message.answer("⚠️ Iltimos, tugmalardan birini tanlang.")

    except Exception as e:
        logging.exception("confirm_edit_handler xatolik: %s", e)
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(RegisterStates.waiting_main_phone, F.contact)
async def main_phone_contact_handler(message: Message, state: FSMContext):
    try:
        if not message.from_user or not message.contact:
            await message.answer("⚠️ Kontakt topilmadi.")
            return

        if message.contact.user_id and message.contact.user_id != message.from_user.id:
            await message.answer("⚠️ Faqat o'zingizning telefon raqamingizni yuboring.")
            return

        phone = normalize_phone(message.contact.phone_number)
        if not is_valid_uz_phone(phone):
            await message.answer("⚠️ Asosiy raqam noto'g'ri. Faqat <b>+998XXXXXXXXX</b> format.")
            return

        await state.update_data(main_phone=phone)

        await message.answer(
            "☎️ <b>Endi QO'SHIMCHA raqamni yuboring</b>\nFormat: <b>+998XXXXXXXXX</b>",
            reply_markup=contact_keyboard("📲 Qo'shimcha raqamni kontakt qilib yuborish")
        )
        await state.set_state(RegisterStates.waiting_extra_phone)

    except Exception as e:
        logging.exception("main_phone_contact_handler xatolik: %s", e)
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(RegisterStates.waiting_main_phone, F.text)
async def main_phone_text_handler(message: Message, state: FSMContext):
    try:
        text = normalize_text(message.text)

        if text == "❌ Bekor qilish":
            await cancel_flow(message, state)
            return

        phone = normalize_phone(text)
        if not is_valid_uz_phone(phone):
            await message.answer("⚠️ Asosiy raqam noto'g'ri. Masalan: <b>+998901234567</b>")
            return

        await state.update_data(main_phone=phone)

        await message.answer(
            "☎️ <b>Endi QO'SHIMCHA raqamni yuboring</b>\nFormat: <b>+998XXXXXXXXX</b>",
            reply_markup=contact_keyboard("📲 Qo'shimcha raqamni kontakt qilib yuborish")
        )
        await state.set_state(RegisterStates.waiting_extra_phone)

    except Exception as e:
        logging.exception("main_phone_text_handler xatolik: %s", e)
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(RegisterStates.waiting_extra_phone, F.contact)
async def extra_phone_contact_handler(message: Message, state: FSMContext):
    try:
        if not message.from_user or not message.contact:
            await message.answer("⚠️ Kontakt topilmadi.")
            return

        if message.contact.user_id and message.contact.user_id != message.from_user.id:
            await message.answer("⚠️ Faqat o'zingizning telefon raqamingizni yuboring.")
            return

        extra_phone = normalize_phone(message.contact.phone_number)
        if not is_valid_uz_phone(extra_phone):
            await message.answer("⚠️ Qo'shimcha raqam noto'g'ri. Faqat <b>+998XXXXXXXXX</b> format.")
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
        logging.exception("extra_phone_contact_handler xatolik: %s", e)
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(RegisterStates.waiting_extra_phone, F.text)
async def extra_phone_text_handler(message: Message, state: FSMContext):
    try:
        text = normalize_text(message.text)

        if text == "❌ Bekor qilish":
            await cancel_flow(message, state)
            return

        extra_phone = normalize_phone(text)
        if not is_valid_uz_phone(extra_phone):
            await message.answer("⚠️ Qo'shimcha raqam noto'g'ri. Masalan: <b>+998901234567</b>")
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
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(RegisterStates.confirm_save, F.text)
async def confirm_save_handler(message: Message, state: FSMContext):
    try:
        text = normalize_text(message.text)
        data = await state.get_data()

        if text == "✏️ Qayta kiritish":
            await message.answer(
                "📱 <b>Asosiy raqamni qayta kiriting</b>\nFormat: <b>+998XXXXXXXXX</b>",
                reply_markup=contact_keyboard("📲 Asosiy raqamni kontakt qilib yuborish")
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

        education_value = safe_html(data.get("education", ""))
        course_value = safe_html(data.get("course", ""))
        group_value = safe_html(data.get("group", ""))
        student_value = safe_html(data.get("student", ""))
        main_phone_value = safe_html(data.get("main_phone", ""))
        extra_phone_value = safe_html(data.get("extra_phone", ""))

        await message.answer(
            "✅ <b>Ma'lumot muvaffaqiyatli saqlandi</b>\n\n"
            f"🎓 Ta'lim shakli: <b>{education_value}</b>\n"
            f"📚 Kurs: <b>{course_value}</b>\n"
            f"👥 Guruh: <b>{group_value}</b>\n"
            f"🧑‍🎓 Talaba: <b>{student_value}</b>\n"
            f"📱 Asosiy raqam: <b>{main_phone_value}</b>\n"
            f"☎️ Qo'shimcha raqam: <b>{extra_phone_value}</b>\n"
            f"🆔 Telegram ID: <b>{message.from_user.id}</b>",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.clear()

    except Exception as e:
        logging.exception("confirm_save_handler xatolik: %s", e)
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(Command("admin"))
async def admin_handler(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        await message.answer("⛔ Siz admin emassiz.")
        return

    await state.clear()
    await message.answer(
        "🛠 <b>Admin panel</b>\nKerakli bo'limni tanlang:",
        reply_markup=admin_keyboard()
    )


@dp.message(F.text == "📊 Statistika")
async def admin_stats_handler(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    try:
        total_students, total_groups, total_educations = await get_sheet_stats()
        await message.answer(
            "📊 <b>Jadval statistikasi</b>\n\n"
            f"🧑‍🎓 Talabalar soni: <b>{total_students}</b>\n"
            f"👥 Guruhlar soni: <b>{total_groups}</b>\n"
            f"🎓 Ta'lim shakllari soni: <b>{total_educations}</b>"
        )
    except Exception as e:
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(F.text == "🔄 Yangilash")
async def admin_refresh_handler(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    try:
        _ = await get_educations()
        await message.answer("✅ Jadval tekshirildi. Yangi talabalar va o'chirilganlar darrov qabul qilinadi.")
    except Exception as e:
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(F.text == "📋 Ustunlar")
async def admin_columns_handler(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    try:
        worksheet = await get_worksheet()
        headers = await get_headers(worksheet)
        text = "\n".join([f"• {safe_html(h)}" for h in headers]) if headers else "Ustunlar topilmadi"
        await message.answer(f"📋 <b>Jadval ustunlari:</b>\n\n{text}")
    except Exception as e:
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(F.text == "🧾 Loglar")
async def admin_logs_handler(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    try:
        worksheet = await get_worksheet()
        records = await get_all_records(worksheet)
        count_col_name = await get_column_name(worksheet, ["Yuborish soni"])

        if not count_col_name:
            await message.answer("⚠️ 'Yuborish soni' ustuni topilmadi.")
            return

        top = []
        for row in records:
            student_name = normalize_text(row.get("F.I.SH.", "")) or normalize_text(row.get("F.I.SH", ""))
            count_value = normalize_text(row.get(count_col_name, "0")) or "0"
            if student_name:
                try:
                    top.append((student_name, int(count_value)))
                except Exception:
                    top.append((student_name, 0))

        top.sort(key=lambda x: x[1], reverse=True)
        top = top[:10]

        if not top:
            await message.answer("ℹ️ Loglar topilmadi.")
            return

        text = "\n".join([f"{i+1}. {safe_html(name)} — <b>{cnt}</b>" for i, (name, cnt) in enumerate(top)])
        await message.answer(f"🧾 <b>Eng ko'p yuborilganlar</b>\n\n{text}")
    except Exception as e:
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(F.text == "🔎 Qidiruv")
async def admin_search_start_handler(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    await state.set_state(RegisterStates.admin_search)
    await message.answer(
        "🔎 <b>Qidiruv uchun talaba F.I.SH. yoki guruh yozing:</b>",
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
            student_value = safe_html(item["student"])
            group_value = safe_html(item["group"])
            course_value = safe_html(item["course"])
            education_value = safe_html(item["education"])
            main_value = safe_html(item["main_phone"] or "-")
            extra_value = safe_html(item["extra_phone"] or "-")
            tg_id_value = safe_html(item["telegram_id"] or "-")
            count_value = safe_html(item["count"] or "0")

            chunks.append(
                f"🧑‍🎓 <b>{student_value}</b>\n"
                f"👥 {group_value} | 📚 {course_value} | 🎓 {education_value}\n"
                f"📱 {main_value}\n"
                f"☎️ {extra_value}\n"
                f"🆔 {tg_id_value}\n"
                f"🧾 Yuborish soni: <b>{count_value}</b>"
            )

        await message.answer(
            "🔎 <b>Qidiruv natijalari</b>\n\n" + "\n\n".join(chunks),
            reply_markup=admin_keyboard()
        )
        await state.clear()
    except Exception as e:
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(F.text == "🆔 ID bo'yicha qidiruv")
async def admin_search_id_start_handler(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    await state.set_state(RegisterStates.admin_search_by_id)
    await message.answer(
        "🆔 <b>Telegram ID ni yuboring:</b>",
        reply_markup=make_keyboard([], add_cancel=True)
    )


@dp.message(RegisterStates.admin_search_by_id, F.text)
async def admin_search_id_handler(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    text = normalize_text(message.text)
    if text == "❌ Bekor qilish":
        await cancel_flow(message, state)
        return

    try:
        worksheet = await get_worksheet()
        records = await get_all_records(worksheet)

        found = []
        for row in records:
            tg_id = normalize_text(row.get("Telegram ID", ""))
            if tg_id == text:
                fish_value = normalize_text(row.get("F.I.SH.", "")) or normalize_text(row.get("F.I.SH", ""))
                guruh_value = normalize_text(row.get("Guruh", ""))
                kurs_value = normalize_text(row.get("Kurs", ""))
                talim_value = normalize_text(row.get("Ta'lim shakli", ""))
                asosiy_value = normalize_text(row.get("Asosiy nomer", "")) or "-"
                qoshimcha_value = normalize_text(row.get("Qo'shimcha nomer", "")) or "-"

                found.append(
                    f"🧑‍🎓 <b>{safe_html(fish_value)}</b>\n"
                    f"👥 {safe_html(guruh_value)}\n"
                    f"📚 {safe_html(kurs_value)}\n"
                    f"🎓 {safe_html(talim_value)}\n"
                    f"📱 {safe_html(asosiy_value)}\n"
                    f"☎️ {safe_html(qoshimcha_value)}\n"
                    f"🆔 {safe_html(tg_id)}"
                )

        if not found:
            await message.answer("ℹ️ Bu Telegram ID bo‘yicha ma’lumot topilmadi.", reply_markup=admin_keyboard())
            await state.clear()
            return

        await message.answer(
            "🆔 <b>ID bo‘yicha qidiruv natijasi</b>\n\n" + "\n\n".join(found[:10]),
            reply_markup=admin_keyboard()
        )
        await state.clear()
    except Exception as e:
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(F.text == "🕒 Oxirgi yangilanishlar")
async def admin_recent_updates_handler(message: Message):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    try:
        updates = await get_recent_updates(10)
        if not updates:
            await message.answer("ℹ️ Oxirgi yangilanishlar topilmadi.")
            return

        text = "\n\n".join([
            f"🧑‍🎓 <b>{safe_html(item['student'])}</b>\n"
            f"👥 {safe_html(item['group'])}\n"
            f"🕒 {safe_html(item['updated'])}\n"
            f"🆔 {safe_html(item['telegram_id'] or '-')}"
            for item in updates
        ])

        await message.answer(f"🕒 <b>Oxirgi yangilanishlar</b>\n\n{text}")
    except Exception as e:
        await message.answer(f"❌ {safe_html(str(e))}")


@dp.message(F.text == "🏠 Chiqish")
async def admin_exit_handler(message: Message, state: FSMContext):
    if not message.from_user or not is_admin(message.from_user.id):
        return

    await state.clear()
    await message.answer(
        "✅ Admin paneldan chiqildi.",
        reply_markup=ReplyKeyboardRemove()
    )


async def main():
    logging.info("Bot ishga tushdi...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
