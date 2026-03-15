import asyncio
import json
import logging
import os
import re
from typing import List, Optional

import gspread
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
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

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi")

if not GOOGLE_SHEET_NAME:
    raise ValueError("GOOGLE_SHEET_NAME topilmadi")

if not GOOGLE_CREDENTIALS_JSON:
    raise ValueError("GOOGLE_CREDENTIALS_JSON topilmadi")

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


class RegisterStates(StatesGroup):
    choosing_education = State()
    choosing_course = State()
    choosing_group = State()
    choosing_student = State()
    choosing_number_type = State()
    waiting_phone = State()


def get_worksheet():
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(credentials)
    spreadsheet = client.open(GOOGLE_SHEET_NAME)
    return spreadsheet.sheet1


def get_headers(worksheet) -> List[str]:
    return [str(h).strip() for h in worksheet.row_values(1)]


def get_col_index_by_name(worksheet, col_name: str) -> Optional[int]:
    headers = get_headers(worksheet)
    for idx, header in enumerate(headers, start=1):
        if header == col_name:
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


def unique_sorted(values: List[str]) -> List[str]:
    result = []
    for value in values:
        value = str(value).strip()
        if value and value not in result:
            result.append(value)
    return result


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


def normalize_phone(phone: str) -> str:
    phone = str(phone).strip()
    phone = re.sub(r"[^\d+]", "", phone)

    if phone.startswith("998") and not phone.startswith("+"):
        phone = "+" + phone
    elif re.fullmatch(r"\d{9}", phone):
        phone = "+998" + phone

    return phone


def find_student_row_index(worksheet, student: str, course: str, group: str, education: str):
    records = get_all_records(worksheet)

    for row_index, row in enumerate(records, start=2):
        if (
            str(row.get("F.I.SH.", "")).strip() == student
            and str(row.get("Kurs", "")).strip() == course
            and str(row.get("Guruh", "")).strip() == group
            and str(row.get("Ta'lim shakli", "")).strip() == education
        ):
            return row_index

    return None


def save_phone_to_sheet(student: str, course: str, group: str, education: str, number_type: str, phone: str):
    worksheet = get_worksheet()
    ensure_phone_columns(worksheet)

    row_index = find_student_row_index(worksheet, student, course, group, education)
    if not row_index:
        raise ValueError("Talaba topilmadi")

    main_col = get_col_index_by_name(worksheet, "Asosiy nomer")
    extra_col = get_col_index_by_name(worksheet, "Qo'shimcha nomer")

    if number_type == "📱 Asosiy nomer":
        worksheet.update_cell(row_index, main_col, phone)
    elif number_type == "☎️ Qo'shimcha nomer":
        worksheet.update_cell(row_index, extra_col, phone)
    else:
        raise ValueError("Noto'g'ri raqam turi")


def get_educations():
    worksheet = get_worksheet()
    records = get_all_records(worksheet)
    return unique_sorted([str(r.get("Ta'lim shakli", "")).strip() for r in records])


def get_courses(education: str):
    worksheet = get_worksheet()
    records = get_all_records(worksheet)
    return unique_sorted([
        str(r.get("Kurs", "")).strip()
        for r in records
        if str(r.get("Ta'lim shakli", "")).strip() == education
    ])


def get_groups(education: str, course: str):
    worksheet = get_worksheet()
    records = get_all_records(worksheet)
    return unique_sorted([
        str(r.get("Guruh", "")).strip()
        for r in records
        if str(r.get("Ta'lim shakli", "")).strip() == education
        and str(r.get("Kurs", "")).strip() == course
    ])


def get_students(education: str, course: str, group: str):
    worksheet = get_worksheet()
    records = get_all_records(worksheet)
    return unique_sorted([
        str(r.get("F.I.SH.", "")).strip()
        for r in records
        if str(r.get("Ta'lim shakli", "")).strip() == education
        and str(r.get("Kurs", "")).strip() == course
        and str(r.get("Guruh", "")).strip() == group
    ])


async def cancel_flow(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Amal bekor qilindi. /start bosing.", reply_markup=ReplyKeyboardRemove())


@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    try:
        educations = get_educations()
        await state.clear()
        await message.answer(
            "🎓 Ta'lim shaklini tanlang:",
            reply_markup=make_keyboard(educations, per_row=2)
        )
        await state.set_state(RegisterStates.choosing_education)
    except Exception as e:
        logging.exception(e)
        await message.answer("Botni ishga tushirishda xatolik bo'ldi.")


@dp.message(Command("help"))
async def help_handler(message: Message):
    await message.answer("/start bosing")


@dp.message(F.text == "❌ Bekor qilish")
async def cancel_handler(message: Message, state: FSMContext):
    await cancel_flow(message, state)


@dp.message(RegisterStates.choosing_education, F.text)
async def education_handler(message: Message, state: FSMContext):
    education = message.text.strip()
    courses = get_courses(education)

    await state.update_data(education=education)
    await message.answer("📚 Kursni tanlang:", reply_markup=make_keyboard(courses, per_row=3))
    await state.set_state(RegisterStates.choosing_course)


@dp.message(RegisterStates.choosing_course, F.text)
async def course_handler(message: Message, state: FSMContext):
    data = await state.get_data()
    education = data.get("education", "")
    course = message.text.strip()

    groups = get_groups(education, course)
    await state.update_data(course=course)
    await message.answer("👥 Guruhni tanlang:", reply_markup=make_keyboard(groups, per_row=2))
    await state.set_state(RegisterStates.choosing_group)


@dp.message(RegisterStates.choosing_group, F.text)
async def group_handler(message: Message, state: FSMContext):
    data = await state.get_data()
    education = data.get("education", "")
    course = data.get("course", "")
    group = message.text.strip()

    students = get_students(education, course, group)
    await state.update_data(group=group)
    await message.answer("Talabani tanlang:", reply_markup=make_keyboard(students, per_row=1))
    await state.set_state(RegisterStates.choosing_student)


@dp.message(RegisterStates.choosing_student, F.text)
async def student_handler(message: Message, state: FSMContext):
    student = message.text.strip()
    await state.update_data(student=student)
    await message.answer("Nomer turini tanlang:", reply_markup=number_type_keyboard())
    await state.set_state(RegisterStates.choosing_number_type)


@dp.message(RegisterStates.choosing_number_type, F.text)
async def number_type_handler(message: Message, state: FSMContext):
    number_type = message.text.strip()
    await state.update_data(number_type=number_type)
    await message.answer("📞 Telefon raqam yuboring:", reply_markup=contact_keyboard())
    await state.set_state(RegisterStates.waiting_phone)


@dp.message(RegisterStates.waiting_phone, F.contact)
async def contact_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.contact.phone_number)
    data = await state.get_data()

    try:
        save_phone_to_sheet(
            student=data.get("student", ""),
            course=data.get("course", ""),
            group=data.get("group", ""),
            education=data.get("education", ""),
            number_type=data.get("number_type", ""),
            phone=phone
        )
        await message.answer("✅ Saqlandi", reply_markup=ReplyKeyboardRemove())
        await state.clear()
    except Exception as e:
        logging.exception(e)
        await message.answer("Google Sheets ga yozishda xatolik bo'ldi.")


@dp.message(RegisterStates.waiting_phone, F.text)
async def phone_text_handler(message: Message, state: FSMContext):
    text = message.text.strip()

    if text == "❌ Bekor qilish":
        await cancel_flow(message, state)
        return

    phone = normalize_phone(text)
    data = await state.get_data()

    try:
        save_phone_to_sheet(
            student=data.get("student", ""),
            course=data.get("course", ""),
            group=data.get("group", ""),
            education=data.get("education", ""),
            number_type=data.get("number_type", ""),
            phone=phone
        )
        await message.answer("✅ Saqlandi", reply_markup=ReplyKeyboardRemove())
        await state.clear()
    except Exception as e:
        logging.exception(e)
        await message.answer("Google Sheets ga yozishda xatolik bo'ldi.")


@dp.message()
async def fallback_handler(message: Message):
    await message.answer("Qayta boshlash uchun /start bosing.")


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
