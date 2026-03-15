import asyncio
import json
import logging
import os
import re
from typing import List, Optional

import gspread
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi")

if not GOOGLE_SHEET_NAME:
    raise ValueError("GOOGLE_SHEET_NAME topilmadi")

if not GOOGLE_CREDENTIALS_JSON:
    raise ValueError("GOOGLE_CREDENTIALS_JSON topilmadi")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


class RegisterStates(StatesGroup):
    choosing_education = State()
    choosing_course = State()
    choosing_group = State()
    choosing_student = State()
    choosing_number_type = State()
    waiting_phone = State()


def normalize_text(value: str) -> str:
    return str(value).strip()


def normalize_header(text: str) -> str:
    return (
        str(text)
        .strip()
        .lower()
        .replace("’", "'")
        .replace("`", "'")
        .replace("ʻ", "'")
    )


def unique_sorted(values: List[str]) -> List[str]:
    result = []
    for value in values:
        value = normalize_text(value)
        if value and value not in result:
            result.append(value)
    return result


def normalize_phone(phone: str) -> str:
    phone = str(phone).strip()
    phone = re.sub(r"[^\d+]", "", phone)

    if phone.startswith("998") and not phone.startswith("+"):
        phone = "+" + phone
    elif re.fullmatch(r"\d{9}", phone):
        phone = "+998" + phone

    return phone


def get_credentials():
    raw_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not raw_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON topilmadi")

    try:
        creds_dict = json.loads(raw_json)
    except Exception as e:
        raise ValueError(f"GOOGLE_CREDENTIALS_JSON noto'g'ri formatda: {e}")

    if "private_key" in creds_dict:
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    return Credentials.from_service_account_info(creds_dict, scopes=scopes)


def get_worksheet():
    try:
        credentials = get_credentials()
        client = gspread.authorize(credentials)
        spreadsheet = client.open(GOOGLE_SHEET_NAME)
        return spreadsheet.sheet1
    except Exception as e:
        raise ValueError(f"Google Sheets ulanishida xatolik: {e}")


def get_headers(worksheet) -> List[str]:
    return [str(h).strip() for h in worksheet.row_values(1)]


def get_column_name(worksheet, possible_names: List[str]) -> Optional[str]:
    headers = get_headers(worksheet)
    normalized_headers = {normalize_header(h): h for h in headers}

    for name in possible_names:
        key = normalize_header(name)
        if key in normalized_headers:
            return normalized_headers[key]

    return None


def get_col_index_by_name(worksheet, col_name: str) -> Optional[int]:
    headers = get_headers(worksheet)
    for idx, header in enumerate(headers, start=1):
        if normalize_header(header) == normalize_header(col_name):
            return idx
    return None


def ensure_phone_columns(worksheet):
    headers = get_headers(worksheet)

    if "Asosiy nomer" not in headers:
        worksheet.update_cell(1, len(headers) + 1, "Asosiy nomer")
        headers.append("Asosiy nomer")

    if "Qo'shimcha nomer" not in headers:
        worksheet.update_cell(1, len(headers) + 1, "Qo'shimcha nomer")


def get_all_records(worksheet):
    return worksheet.get_all_records()


def get_required_columns(worksheet):
    education_col = get_column_name(
        worksheet,
        ["Ta'lim shakli", "Ta’lim shakli", "Talim shakli", "talim shakli"]
    )
    course_col = get_column_name(worksheet, ["Kurs", "kurs"])
    group_col = get_column_name(worksheet, ["Guruh", "guruh", "Group", "group"])
    student_col = get_column_name(
        worksheet,
        ["F.I.SH.", "F.I.SH", "FISH", "Fish", "FIO", "Talaba"]
    )

    if not education_col:
        raise ValueError("Google Sheetsda 'Ta'lim shakli' ustuni topilmadi")
    if not course_col:
        raise ValueError("Google Sheetsda 'Kurs' ustuni topilmadi")
    if not group_col:
        raise ValueError("Google Sheetsda 'Guruh' ustuni topilmadi")
    if not student_col:
        raise ValueError("Google Sheetsda 'F.I.SH.' ustuni topilmadi")

    return {
        "education": education_col,
        "course": course_col,
        "group": group_col,
        "student": student_col,
    }


def get_educations():
    worksheet = get_worksheet()
    records = get_all_records(worksheet)
    columns = get_required_columns(worksheet)

    values = [str(r.get(columns["education"], "")).strip() for r in records]
    return unique_sorted(values)


def get_courses(education: str):
    worksheet = get_worksheet()
    records = get_all_records(worksheet)
    columns = get_required_columns(worksheet)

    return unique_sorted([
        str(r.get(columns["course"], "")).strip()
        for r in records
        if str(r.get(columns["education"], "")).strip() == education
    ])


def get_groups(education: str, course: str):
    worksheet = get_worksheet()
    records = get_all_records(worksheet)
    columns = get_required_columns(worksheet)

    return unique_sorted([
        str(r.get(columns["group"], "")).strip()
        for r in records
        if str(r.get(columns["education"], "")).strip() == education
        and str(r.get(columns["course"], "")).strip() == course
    ])


def get_students(education: str, course: str, group: str):
    worksheet = get_worksheet()
    records = get_all_records(worksheet)
    columns = get_required_columns(worksheet)

    return unique_sorted([
        str(r.get(columns["student"], "")).strip()
        for r in records
        if str(r.get(columns["education"], "")).strip() == education
        and str(r.get(columns["course"], "")).strip() == course
        and str(r.get(columns["group"], "")).strip() == group
    ])


def find_student_row_index(worksheet, student: str, course: str, group: str, education: str):
    records = get_all_records(worksheet)
    columns = get_required_columns(worksheet)

    for row_index, row in enumerate(records, start=2):
        if (
            str(row.get(columns["student"], "")).strip() == student
            and str(row.get(columns["course"], "")).strip() == course
            and str(row.get(columns["group"], "")).strip() == group
            and str(row.get(columns["education"], "")).strip() == education
        ):
            return row_index

    return None


def save_phone_to_sheet(student: str, course: str, group: str, education: str, number_type: str, phone: str):
    worksheet = get_worksheet()
    ensure_phone_columns(worksheet)

    row_index = find_student_row_index(
        worksheet=worksheet,
        student=student,
        course=course,
        group=group,
        education=education
    )

    if not row_index:
        raise ValueError("Talaba topilmadi")

    main_col = get_col_index_by_name(worksheet, "Asosiy nomer")
    extra_col = get_col_index_by_name(worksheet, "Qo'shimcha nomer")

    if not main_col or not extra_col:
        raise ValueError("Telefon ustunlari topilmadi")

    if number_type == "📱 Asosiy nomer":
        worksheet.update_cell(row_index, main_col, phone)
    elif number_type == "☎️ Qo'shimcha nomer":
        worksheet.update_cell(row_index, extra_col, phone)
    else:
        raise ValueError("Noto'g'ri raqam turi")


def make_keyboard(items: List[str], per_row: int = 2, add_cancel: bool = True):
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


def number_type_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Asosiy nomer"), KeyboardButton(text="☎️ Qo'shimcha nomer")],
            [KeyboardButton(text="❌ Bekor qilish")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )


def contact_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📲 Kontakt yuborish", request_contact=True)],
            [KeyboardButton(text="❌ Bekor qilish")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )


async def cancel_flow(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "❌ Amal bekor qilindi.\nQayta boshlash uchun /start bosing.",
        reply_markup=ReplyKeyboardRemove()
    )


@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    try:
        educations = get_educations()

        if not educations:
            await message.answer("⚠️ Ta'lim shakllari topilmadi.")
            return

        await state.clear()
        await message.answer(
            "🎓 Ta'lim shaklini tanlang:",
            reply_markup=make_keyboard(educations, per_row=2)
        )
        await state.set_state(RegisterStates.choosing_education)

    except Exception as e:
        logging.exception("start_handler xatolik: %s", e)
        await message.answer(f"❌ {e}")


@dp.message(Command("help"))
async def help_handler(message: Message):
    await message.answer(
        "ℹ️ Foydalanish tartibi:\n\n"
        "1. /start bosing\n"
        "2. 🎓 Ta'lim shaklini tanlang\n"
        "3. 📚 Kursni tanlang\n"
        "4. 👥 Guruhni tanlang\n"
        "5. 🧑‍🎓 Talabani tanlang\n"
        "6. 📱 yoki ☎️ nomer turini tanlang\n"
        "7. 📲 Kontakt yoki telefon raqam yuboring"
    )


@dp.message(F.text == "❌ Bekor qilish")
async def cancel_handler(message: Message, state: FSMContext):
    await cancel_flow(message, state)


@dp.message(RegisterStates.choosing_education, F.text)
async def education_handler(message: Message, state: FSMContext):
    try:
        education = message.text.strip()
        educations = get_educations()

        if education not in educations:
            await message.answer("⚠️ Iltimos, tugmalardan birini tanlang.")
            return

        courses = get_courses(education)

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
        await message.answer(f"❌ {e}")


@dp.message(RegisterStates.choosing_course, F.text)
async def course_handler(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        education = data.get("education", "")
        course = message.text.strip()

        courses = get_courses(education)
        if course not in courses:
            await message.answer("⚠️ Iltimos, kursni tugmalardan tanlang.")
            return

        groups = get_groups(education, course)
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
        await message.answer(f"❌ {e}")


@dp.message(RegisterStates.choosing_group, F.text)
async def group_handler(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        education = data.get("education", "")
        course = data.get("course", "")
        group = message.text.strip()

        groups = get_groups(education, course)
        if group not in groups:
            await message.answer("⚠️ Iltimos, guruhni tugmalardan tanlang.")
            return

        students = get_students(education, course, group)
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
        await message.answer(f"❌ {e}")


@dp.message(RegisterStates.choosing_student, F.text)
async def student_handler(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        education = data.get("education", "")
        course = data.get("course", "")
        group = data.get("group", "")
        student = message.text.strip()

        students = get_students(education, course, group)
        if student not in students:
            await message.answer("⚠️ Iltimos, talabani tugmalardan tanlang.")
            return

        await state.update_data(student=student)
        await message.answer(
            f"✅ Tanlangan talaba: {student}\n\n📞 Nomer turini tanlang:",
            reply_markup=number_type_keyboard()
        )
        await state.set_state(RegisterStates.choosing_number_type)

    except Exception as e:
        logging.exception("student_handler xatolik: %s", e)
        await message.answer(f"❌ {e}")


@dp.message(RegisterStates.choosing_number_type, F.text)
async def number_type_handler(message: Message, state: FSMContext):
    try:
        number_type = message.text.strip()

        if number_type not in ["📱 Asosiy nomer", "☎️ Qo'shimcha nomer"]:
            await message.answer("⚠️ Iltimos, tugmalardan birini tanlang.")
            return

        await state.update_data(number_type=number_type)
        await message.answer(
            "📲 Endi kontakt yoki telefon raqam yuboring:",
            reply_markup=contact_keyboard()
        )
        await state.set_state(RegisterStates.waiting_phone)

    except Exception as e:
        logging.exception("number_type_handler xatolik: %s", e)
        await message.answer(f"❌ {e}")


@dp.message(RegisterStates.waiting_phone, F.contact)
async def contact_handler(message: Message, state: FSMContext):
    try:
        if not message.contact:
            await message.answer("⚠️ Kontakt topilmadi.")
            return

        phone = normalize_phone(message.contact.phone_number)
        data = await state.get_data()

        save_phone_to_sheet(
            student=data.get("student", ""),
            course=data.get("course", ""),
            group=data.get("group", ""),
            education=data.get("education", ""),
            number_type=data.get("number_type", ""),
            phone=phone
        )

        await message.answer(
            "✅ Telefon raqam muvaffaqiyatli saqlandi.",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.clear()

    except Exception as e:
        logging.exception("contact_handler xatolik: %s", e)
        await message.answer(f"❌ {e}")


@dp.message(RegisterStates.waiting_phone, F.text)
async def phone_text_handler(message: Message, state: FSMContext):
    try:
        text = message.text.strip()

        if text == "❌ Bekor qilish":
            await cancel_flow(message, state)
            return

        phone = normalize_phone(text)

        if not re.fullmatch(r"^\+?\d{9,15}$", phone):
            await message.answer("⚠️ Telefon raqam noto'g'ri. Masalan: +998901234567")
            return

        data = await state.get_data()

        save_phone_to_sheet(
            student=data.get("student", ""),
            course=data.get("course", ""),
            group=data.get("group", ""),
            education=data.get("education", ""),
            number_type=data.get("number_type", ""),
            phone=phone
        )

        await message.answer(
            "✅ Telefon raqam muvaffaqiyatli saqlandi.",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.clear()

    except Exception as e:
        logging.exception("phone_text_handler xatolik: %s", e)
        await message.answer(f"❌ {e}")


@dp.message()
async def fallback_handler(message: Message):
    await message.answer("ℹ️ Qayta boshlash uchun /start bosing.")


async def main():
    logging.info("Bot ishga tushdi...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
