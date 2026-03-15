import asyncio
import logging
import os
import re
from datetime import datetime
from typing import Dict, List, Optional

import gspread
import pandas as pd
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
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

# =========================
# CONFIG
# =========================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
EXCEL_FILE = os.getenv("EXCEL_FILE", "students.xlsx")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "StudentPhones")
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
GOOGLE_WORKSHEET_NAME = os.getenv("GOOGLE_WORKSHEET_NAME", "Telefonlar")

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi. .env yoki Railway Variables ga qo'ying.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Excel cache
students_df: pd.DataFrame = pd.DataFrame()

# =========================
# FSM
# =========================
class RegisterStates(StatesGroup):
    choosing_education = State()
    choosing_course = State()
    choosing_group = State()
    choosing_student = State()
    choosing_number_type = State()
    waiting_phone = State()


# =========================
# HELPERS
# =========================
def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_phone(phone: str) -> str:
    phone = str(phone).strip()
    phone = re.sub(r"[^\d+]", "", phone)

    if phone.startswith("998") and not phone.startswith("+"):
        phone = "+" + phone
    elif phone.startswith("9") and len(phone) == 9:
        phone = "+998" + phone
    elif phone.startswith("8") and len(phone) >= 9:
        # kerak bo'lsa o'zgartirsa bo'ladi
        pass

    return phone


def make_reply_keyboard(items: List[str], buttons_per_row: int = 2, add_cancel: bool = True) -> ReplyKeyboardMarkup:
    keyboard = []
    row = []

    for item in items:
        row.append(KeyboardButton(text=item))
        if len(row) == buttons_per_row:
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


def contact_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📲 Kontakt yuborish", request_contact=True)],
            [KeyboardButton(text="❌ Bekor qilish")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )


def load_students_excel() -> pd.DataFrame:
    if not os.path.exists(EXCEL_FILE):
        raise FileNotFoundError(f"{EXCEL_FILE} fayli topilmadi.")

    df = pd.read_excel(EXCEL_FILE)
    df.columns = [str(col).strip() for col in df.columns]

    # barcha qiymatlarni silliqlab olamiz
    for col in df.columns:
        df[col] = df[col].apply(normalize_text)

    return df


def find_column(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    normalized_map = {col.lower().strip(): col for col in df.columns}

    for alias in aliases:
        key = alias.lower().strip()
        if key in normalized_map:
            return normalized_map[key]

    return None


def get_required_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Excel ustunlarini topadi.
    Kerak bo'lsa aliases ga yana qo'shimcha nomlar qo'shsa bo'ladi.
    """
    education_col = find_column(df, [
        "Ta'lim shakli", "Talim shakli", "ta'lim shakli", "talim shakli"
    ])
    course_col = find_column(df, [
        "Kurs", "kurs"
    ])
    group_col = find_column(df, [
        "Guruh", "Group", "group", "guruh"
    ])
    student_col = find_column(df, [
        "F.I.SH", "FISH", "Fish", "Talaba", "Student", "Student Name",
        "Full Name", "Ism familiya", "Familiya Ism", "FIO"
    ])

    missing = []
    if not education_col:
        missing.append("Ta'lim shakli")
    if not course_col:
        missing.append("Kurs")
    if not group_col:
        missing.append("Guruh")
    if not student_col:
        missing.append("Talaba F.I.SH")

    if missing:
        raise ValueError(
            "Excel faylda kerakli ustun(lar) topilmadi: " + ", ".join(missing)
        )

    return {
        "education": education_col,
        "course": course_col,
        "group": group_col,
        "student": student_col,
    }


def get_unique_values(df: pd.DataFrame, column: str) -> List[str]:
    values = [normalize_text(v) for v in df[column].dropna().tolist()]
    values = [v for v in values if v]
    unique_values = sorted(set(values), key=lambda x: str(x))
    return unique_values


def get_students_for_group(
    df: pd.DataFrame,
    education: str,
    course: str,
    group: str,
    columns: Dict[str, str]
) -> List[str]:
    filtered = df[
        (df[columns["education"]] == education) &
        (df[columns["course"]] == course) &
        (df[columns["group"]] == group)
    ]

    students = filtered[columns["student"]].dropna().tolist()
    students = [normalize_text(s) for s in students if normalize_text(s)]
    students = sorted(set(students))
    return students


def get_google_worksheet():
    client = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
    spreadsheet = client.open(GOOGLE_SHEET_NAME)

    try:
        worksheet = spreadsheet.worksheet(GOOGLE_WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=GOOGLE_WORKSHEET_NAME, rows=2000, cols=15)
        worksheet.append_row([
            "Timestamp",
            "Telegram ID",
            "Telegram Full Name",
            "Username",
            "Ta'lim shakli",
            "Kurs",
            "Guruh",
            "Talaba",
            "Raqam turi",
            "Telefon raqam"
        ])

    return worksheet


def save_to_google_sheets(data: Dict[str, str]) -> None:
    worksheet = get_google_worksheet()
    records = worksheet.get_all_records()

    existing_row_index = None
    for i, row in enumerate(records, start=2):  # 1-qator header
        if (
            str(row.get("Talaba", "")).strip() == data["student"]
            and str(row.get("Raqam turi", "")).strip() == data["number_type"]
        ):
            existing_row_index = i
            break

    row_values = [
        data["timestamp"],
        data["telegram_id"],
        data["telegram_full_name"],
        data["username"],
        data["education"],
        data["course"],
        data["group"],
        data["student"],
        data["number_type"],
        data["phone"],
    ]

    if existing_row_index:
        worksheet.update(f"A{existing_row_index}:J{existing_row_index}", [row_values])
    else:
        worksheet.append_row(row_values)


def get_number_type_buttons() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Asosiy nomer"), KeyboardButton(text="☎️ Qo'shimcha nomer")],
            [KeyboardButton(text="❌ Bekor qilish")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )


async def cancel_flow(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Amal bekor qilindi. Qayta boshlash uchun /start bosing.",
        reply_markup=ReplyKeyboardRemove()
    )


# =========================
# STARTUP
# =========================
async def on_startup():
    global students_df
    students_df = load_students_excel()
    logging.info("students.xlsx yuklandi. Qatorlar soni: %s", len(students_df))


# =========================
# HANDLERS
# =========================
@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    try:
        if students_df.empty:
            await message.answer(
                "Talabalar fayli topilmadi yoki bo'sh. Admin bilan bog'laning."
            )
            return

        columns = get_required_columns(students_df)
        educations = get_unique_values(students_df, columns["education"])

        if not educations:
            await message.answer("Ta'lim shakllari topilmadi.")
            return

        await state.clear()
        await state.update_data(columns=columns)

        await message.answer(
            "🎓 Ta'lim shaklini tanlang:",
            reply_markup=make_reply_keyboard(educations, buttons_per_row=2)
        )
        await state.set_state(RegisterStates.choosing_education)

    except Exception as e:
        logging.exception("start_handler xatolik: %s", e)
        await message.answer("Botni ishga tushirishda xatolik bo'ldi.")


@dp.message(Command("help"))
async def help_handler(message: Message):
    await message.answer(
        "Botdan foydalanish tartibi:\n\n"
        "1. /start bosing\n"
        "2. Ta'lim shaklini tanlang\n"
        "3. Kursni tanlang\n"
        "4. Guruhni tanlang\n"
        "5. Talabani tanlang\n"
        "6. Raqam turini tanlang\n"
        "7. Kontakt yuboring"
    )


@dp.message(F.text == "❌ Bekor qilish")
async def cancel_handler(message: Message, state: FSMContext):
    await cancel_flow(message, state)


# =========================
# CHOOSE EDUCATION
# =========================
@dp.message(RegisterStates.choosing_education, F.text)
async def education_handler(message: Message, state: FSMContext):
    data = await state.get_data()
    columns = data["columns"]
    education = message.text.strip()

    available = get_unique_values(students_df, columns["education"])
    if education not in available:
        await message.answer("Iltimos, tugmalardan birini tanlang.")
        return

    filtered = students_df[students_df[columns["education"]] == education]
    courses = get_unique_values(filtered, columns["course"])

    if not courses:
        await message.answer("Bu ta'lim shakli uchun kurslar topilmadi.")
        return

    await state.update_data(education=education)

    await message.answer(
        "📚 Kursni tanlang:",
        reply_markup=make_reply_keyboard(courses, buttons_per_row=3)
    )
    await state.set_state(RegisterStates.choosing_course)


# =========================
# CHOOSE COURSE
# =========================
@dp.message(RegisterStates.choosing_course, F.text)
async def course_handler(message: Message, state: FSMContext):
    data = await state.get_data()
    columns = data["columns"]
    education = data["education"]
    course = message.text.strip()

    filtered = students_df[students_df[columns["education"]] == education]
    courses = get_unique_values(filtered, columns["course"])

    if course not in courses:
        await message.answer("Iltimos, kursni tugmalardan tanlang.")
        return

    filtered = students_df[
        (students_df[columns["education"]] == education) &
        (students_df[columns["course"]] == course)
    ]
    groups = get_unique_values(filtered, columns["group"])

    if not groups:
        await message.answer("Bu kurs uchun guruhlar topilmadi.")
        return

    await state.update_data(course=course)

    await message.answer(
        "👥 Guruhni tanlang:",
        reply_markup=make_reply_keyboard(groups, buttons_per_row=2)
    )
    await state.set_state(RegisterStates.choosing_group)


# =========================
# CHOOSE GROUP
# =========================
@dp.message(RegisterStates.choosing_group, F.text)
async def group_handler(message: Message, state: FSMContext):
    data = await state.get_data()
    columns = data["columns"]
    education = data["education"]
    course = data["course"]
    group = message.text.strip()

    filtered = students_df[
        (students_df[columns["education"]] == education) &
        (students_df[columns["course"]] == course)
    ]
    groups = get_unique_values(filtered, columns["group"])

    if group not in groups:
        await message.answer("Iltimos, guruhni tugmalardan tanlang.")
        return

    students = get_students_for_group(
        students_df,
        education=education,
        course=course,
        group=group,
        columns=columns
    )

    if not students:
        await message.answer("Bu guruhda talabalar topilmadi.")
        return

    await state.update_data(group=group)

    students_text = "\n".join([f"• {s}" for s in students[:50]])
    await message.answer(
        f"🧑‍🎓 {group} guruhidagi talabalar:\n\n{students_text}"
    )

    await message.answer(
        "Talabani tanlang:",
        reply_markup=make_reply_keyboard(students, buttons_per_row=1)
    )
    await state.set_state(RegisterStates.choosing_student)


# =========================
# CHOOSE STUDENT
# =========================
@dp.message(RegisterStates.choosing_student, F.text)
async def student_handler(message: Message, state: FSMContext):
    data = await state.get_data()
    columns = data["columns"]
    education = data["education"]
    course = data["course"]
    group = data["group"]
    student = message.text.strip()

    students = get_students_for_group(
        students_df,
        education=education,
        course=course,
        group=group,
        columns=columns
    )

    if student not in students:
        await message.answer("Iltimos, talabani tugmalardan tanlang.")
        return

    await state.update_data(student=student)

    await message.answer(
        f"✅ Tanlangan talaba: {student}\n\nRaqam turini tanlang:",
        reply_markup=get_number_type_buttons()
    )
    await state.set_state(RegisterStates.choosing_number_type)


# =========================
# CHOOSE NUMBER TYPE
# =========================
@dp.message(RegisterStates.choosing_number_type, F.text)
async def number_type_handler(message: Message, state: FSMContext):
    text = message.text.strip()

    allowed = ["📱 Asosiy nomer", "☎️ Qo'shimcha nomer"]
    if text not in allowed:
        await message.answer("Iltimos, pastdagi tugmalardan birini tanlang.")
        return

    await state.update_data(number_type=text)

    await message.answer(
        "📞 Endi telefon raqamni kontakt ko'rinishida yuboring:",
        reply_markup=contact_keyboard()
    )
    await state.set_state(RegisterStates.waiting_phone)


# =========================
# PHONE RECEIVE
# =========================
@dp.message(RegisterStates.waiting_phone, F.contact)
async def contact_phone_handler(message: Message, state: FSMContext):
    contact = message.contact

    if not contact:
        await message.answer("Kontakt topilmadi.")
        return

    phone = normalize_phone(contact.phone_number)
    data = await state.get_data()

    payload = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "telegram_id": str(message.from_user.id) if message.from_user else "",
        "telegram_full_name": message.from_user.full_name if message.from_user else "",
        "username": message.from_user.username if message.from_user and message.from_user.username else "",
        "education": data.get("education", ""),
        "course": data.get("course", ""),
        "group": data.get("group", ""),
        "student": data.get("student", ""),
        "number_type": data.get("number_type", ""),
        "phone": phone,
    }

    try:
        save_to_google_sheets(payload)

        await message.answer(
            "✅ Ma'lumot muvaffaqiyatli saqlandi.\n\n"
            f"🎓 Ta'lim shakli: {payload['education']}\n"
            f"📚 Kurs: {payload['course']}\n"
            f"👥 Guruh: {payload['group']}\n"
            f"🧑‍🎓 Talaba: {payload['student']}\n"
            f"🏷 Raqam turi: {payload['number_type']}\n"
            f"📞 Telefon: {payload['phone']}",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.clear()

    except Exception as e:
        logging.exception("Google Sheets ga yozishda xatolik: %s", e)
        await message.answer(
            "Ma'lumotni Google Sheets ga yozishda xatolik bo'ldi.",
            reply_markup=ReplyKeyboardRemove()
        )


@dp.message(RegisterStates.waiting_phone, F.text)
async def text_phone_handler(message: Message, state: FSMContext):
    text = message.text.strip()

    if text == "❌ Bekor qilish":
        await cancel_flow(message, state)
        return

    phone = normalize_phone(text)

    if not re.fullmatch(r"^\+?\d{9,15}$", phone):
        await message.answer(
            "Telefon raqam noto'g'ri formatda.\n"
            "Kontakt yuboring yoki raqamni to'g'ri kiriting.\n\n"
            "Masalan: +998901234567"
        )
        return

    data = await state.get_data()

    payload = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "telegram_id": str(message.from_user.id) if message.from_user else "",
        "telegram_full_name": message.from_user.full_name if message.from_user else "",
        "username": message.from_user.username if message.from_user and message.from_user.username else "",
        "education": data.get("education", ""),
        "course": data.get("course", ""),
        "group": data.get("group", ""),
        "student": data.get("student", ""),
        "number_type": data.get("number_type", ""),
        "phone": phone,
    }

    try:
        save_to_google_sheets(payload)

        await message.answer(
            "✅ Ma'lumot muvaffaqiyatli saqlandi.\n\n"
            f"🎓 Ta'lim shakli: {payload['education']}\n"
            f"📚 Kurs: {payload['course']}\n"
            f"👥 Guruh: {payload['group']}\n"
            f"🧑‍🎓 Talaba: {payload['student']}\n"
            f"🏷 Raqam turi: {payload['number_type']}\n"
            f"📞 Telefon: {payload['phone']}",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.clear()

    except Exception as e:
        logging.exception("Google Sheets ga yozishda xatolik: %s", e)
        await message.answer(
            "Ma'lumotni Google Sheets ga yozishda xatolik bo'ldi.",
            reply_markup=ReplyKeyboardRemove()
        )


# =========================
# FALLBACK
# =========================
@dp.message()
async def fallback_handler(message: Message):
    await message.answer(
        "Qayta boshlash uchun /start bosing."
    )


# =========================
# MAIN
# =========================
async def main():
    await on_startup()
    logging.info("Bot ishga tushdi...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot to'xtatildi.")
