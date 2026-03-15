import asyncio
import logging
import os
import re
from datetime import datetime
from io import BytesIO

import gspread
import pandas as pd
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
}

SPREADSHEET_ID = "1We8s8NcH4fDCHgH53EsGeO19g6Z2I9nMPBLppo8cTBA"
WORKSHEET_NAME = "Sheet1"

EXCEL_FILE = "students.xlsx"
CREDENTIALS_FILE = "credentials.json"

REQUIRED_COLUMNS = ["Ta'lim shakli", "Kurs", "Guruh", "F.I.SH."]

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi")


# =========================
# YORDAMCHI FUNKSIYALAR
# =========================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def normalize_phone(phone_text: str) -> str | None:
    """
    901234567 -> +998901234567
    998901234567 -> +998901234567
    +998901234567 -> +998901234567
    """
    phone = phone_text.strip()
    phone = re.sub(r"[^\d+]", "", phone)

    if re.fullmatch(r"\+998\d{9}", phone):
        return phone
    if re.fullmatch(r"998\d{9}", phone):
        return "+" + phone
    if re.fullmatch(r"\d{9}", phone):
        return "+998" + phone

    return None


def make_keyboard(items, row_width=2, add_cancel=True):
    rows = []
    row = []

    for item in items:
        row.append(KeyboardButton(text=str(item)))
        if len(row) == row_width:
            rows.append(row)
            row = []

    if row:
        rows.append(row)

    if add_cancel:
        rows.append([KeyboardButton(text="❌ Bekor qilish")])

    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def load_students() -> pd.DataFrame:
    if not os.path.exists(EXCEL_FILE):
        raise FileNotFoundError(f"{EXCEL_FILE} topilmadi")

    df = pd.read_excel(EXCEL_FILE)
    df.columns = [str(c).strip() for c in df.columns]

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"{EXCEL_FILE} ichida '{col}' ustuni yo'q")

    df = df[REQUIRED_COLUMNS].copy().dropna()

    for col in REQUIRED_COLUMNS:
        df[col] = df[col].astype(str).str.strip()

    df = df[
        (df["Ta'lim shakli"] != "")
        & (df["Kurs"] != "")
        & (df["Guruh"] != "")
        & (df["F.I.SH."] != "")
    ].copy()

    return df


# =========================
# GOOGLE SHEETS
# =========================
def connect_sheet():
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(f"{CREDENTIALS_FILE} topilmadi")

    creds = Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )

    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
    return worksheet


sheet = connect_sheet()


def ensure_sheet_header():
    values = sheet.get_all_values()
    header = [
        "created_at",
        "updated_at",
        "telegram_user_id",
        "telegram_full_name",
        "telegram_username",
        "Ta'lim shakli",
        "Kurs",
        "Guruh",
        "F.I.SH.",
        "Asosiy raqam",
        "Qo'shimcha raqam",
    ]
    if not values:
        sheet.append_row(header)


def get_all_records():
    return sheet.get_all_records()


def find_user_row(telegram_user_id: int):
    records = get_all_records()
    for idx, row in enumerate(records, start=2):
        if str(row.get("telegram_user_id", "")).strip() == str(telegram_user_id):
            return idx, row
    return None, None


def student_already_exists(talim: str, kurs: str, guruh: str, fio: str) -> bool:
    records = get_all_records()
    for row in records:
        if (
            str(row.get("Ta'lim shakli", "")).strip() == talim
            and str(row.get("Kurs", "")).strip() == kurs
            and str(row.get("Guruh", "")).strip() == guruh
            and str(row.get("F.I.SH.", "")).strip() == fio
        ):
            return True
    return False


def save_result(data: dict):
    sheet.append_row(
        [
            data["created_at"],
            data["updated_at"],
            data["telegram_user_id"],
            data["telegram_full_name"],
            data["telegram_username"],
            data["Ta'lim shakli"],
            data["Kurs"],
            data["Guruh"],
            data["F.I.SH."],
            data["Asosiy raqam"],
            data["Qo'shimcha raqam"],
        ]
    )


def update_user_phones(telegram_user_id: int, main_phone: str, extra_phone: str) -> bool:
    row_number, _ = find_user_row(telegram_user_id)
    if not row_number:
        return False

    # 2=updated_at, 10=Asosiy raqam, 11=Qo'shimcha raqam
    sheet.update_cell(row_number, 2, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    sheet.update_cell(row_number, 10, main_phone)
    sheet.update_cell(row_number, 11, extra_phone)
    return True


students_df = load_students()


# =========================
# STATES
# =========================
class RegisterForm(StatesGroup):
    talim = State()
    kurs = State()
    guruh = State()
    fio = State()
    phone1 = State()
    phone2 = State()


class EditForm(StatesGroup):
    phone1 = State()
    phone2 = State()


# =========================
# BOT
# =========================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# =========================
# START / USER BUYRUQLAR
# =========================
@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    await state.clear()

    _, existing = find_user_row(message.from_user.id)
    if existing:
        await message.answer(
            "✅ Siz allaqachon ma'lumot yuborgansiz.\n\n"
            "📄 Ko‘rish: /myinfo\n"
            "✏️ Tahrirlash: /edit"
        )
        return

    talim_list = sorted(students_df["Ta'lim shakli"].unique().tolist())
    if not talim_list:
        await message.answer("⚠️ Talabalar ro'yxati hali kiritilmagan.")
        return

    await state.set_state(RegisterForm.talim)
    await message.answer(
        "👋 Assalomu alaykum!\n\n🎓 Ta'lim shaklini tanlang:",
        reply_markup=make_keyboard(talim_list, row_width=2),
    )


@dp.message(Command("cancel"))
async def cancel_command(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "❌ Jarayon bekor qilindi.\nQayta boshlash uchun /start bosing.",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(F.text == "❌ Bekor qilish")
async def cancel_button(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "❌ Jarayon bekor qilindi.\nQayta boshlash uchun /start bosing.",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(Command("id"))
async def id_handler(message: Message):
    await message.answer(f"🆔 Sizning Telegram ID: {message.from_user.id}")


@dp.message(Command("myinfo"))
async def myinfo_handler(message: Message):
    _, row = find_user_row(message.from_user.id)

    if not row:
        await message.answer("ℹ️ Siz hali ma'lumot yubormagansiz. /start bosing.")
        return

    text = (
        "📄 Sizning ma'lumotlaringiz:\n\n"
        f"🎓 Ta'lim shakli: {row.get('Ta'lim shakli', '')}\n"
        f"📚 Kurs: {row.get('Kurs', '')}\n"
        f"👥 Guruh: {row.get('Guruh', '')}\n"
        f"🧑‍🎓 F.I.SH.: {row.get('F.I.SH.', '')}\n"
        f"📱 Asosiy raqam: {row.get('Asosiy raqam', '')}\n"
        f"📲 Qo'shimcha raqam: {row.get('Qo'shimcha raqam', '')}"
    )
    await message.answer(text)


@dp.message(Command("edit"))
async def edit_handler(message: Message, state: FSMContext):
    _, row = find_user_row(message.from_user.id)

    if not row:
        await message.answer("ℹ️ Siz hali ma'lumot yubormagansiz. /start bosing.")
        return

    await state.clear()
    await state.set_state(EditForm.phone1)
    await message.answer(
        "✏️ Raqamlarni tahrirlash boshlandi.\n\n"
        "📱 Yangi asosiy raqamni kiriting:\n"
        "Masalan: 901234567 yoki +998901234567",
        reply_markup=ReplyKeyboardRemove(),
    )


# =========================
# RO‘YXATDAN O‘TISH
# =========================
@dp.message(RegisterForm.talim)
async def talim_handler(message: Message, state: FSMContext):
    value = message.text.strip()
    talim_list = sorted(students_df["Ta'lim shakli"].unique().tolist())

    if value not in talim_list:
        await message.answer("⚠️ Ro'yxatdan birini tanlang.")
        return

    await state.update_data(talim=value)

    courses = sorted(
        students_df[students_df["Ta'lim shakli"] == value]["Kurs"].unique().tolist(),
        key=lambda x: str(x),
    )

    await state.set_state(RegisterForm.kurs)
    await message.answer(
        "📚 Kursingizni tanlang:",
        reply_markup=make_keyboard(courses, row_width=3),
    )


@dp.message(RegisterForm.kurs)
async def kurs_handler(message: Message, state: FSMContext):
    value = message.text.strip()
    data = await state.get_data()

    courses = sorted(
        students_df[students_df["Ta'lim shakli"] == data["talim"]]["Kurs"].unique().tolist(),
        key=lambda x: str(x),
    )
    courses = [str(x) for x in courses]

    if value not in courses:
        await message.answer("⚠️ Ro'yxatdan birini tanlang.")
        return

    await state.update_data(kurs=value)

    groups = sorted(
        students_df[
            (students_df["Ta'lim shakli"] == data["talim"])
            & (students_df["Kurs"].astype(str) == value)
        ]["Guruh"].unique().tolist()
    )

    await state.set_state(RegisterForm.guruh)
    await message.answer(
        "👥 Guruhingizni tanlang:",
        reply_markup=make_keyboard(groups, row_width=3),
    )


@dp.message(RegisterForm.guruh)
async def guruh_handler(message: Message, state: FSMContext):
    value = message.text.strip()
    data = await state.get_data()

    groups = sorted(
        students_df[
            (students_df["Ta'lim shakli"] == data["talim"])
            & (students_df["Kurs"].astype(str) == data["kurs"])
        ]["Guruh"].unique().tolist()
    )

    if value not in groups:
        await message.answer("⚠️ Ro'yxatdan birini tanlang.")
        return

    await state.update_data(guruh=value)

    names = sorted(
        students_df[
            (students_df["Ta'lim shakli"] == data["talim"])
            & (students_df["Kurs"].astype(str) == data["kurs"])
            & (students_df["Guruh"] == value)
        ]["F.I.SH."].unique().tolist()
    )

    await state.set_state(RegisterForm.fio)
    await message.answer(
        "🧑‍🎓 F.I.SH. ni tanlang:",
        reply_markup=make_keyboard(names, row_width=1),
    )


@dp.message(RegisterForm.fio)
async def fio_handler(message: Message, state: FSMContext):
    value = message.text.strip()
    data = await state.get_data()

    names = sorted(
        students_df[
            (students_df["Ta'lim shakli"] == data["talim"])
            & (students_df["Kurs"].astype(str) == data["kurs"])
            & (students_df["Guruh"] == data["guruh"])
        ]["F.I.SH."].unique().tolist()
    )

    if value not in names:
        await message.answer("⚠️ Ro'yxatdan birini tanlang.")
        return

    if student_already_exists(
        talim=data["talim"],
        kurs=data["kurs"],
        guruh=data["guruh"],
        fio=value,
    ):
        await state.clear()
        await message.answer("⚠️ Bu talaba uchun ma'lumot allaqachon kiritilgan.")
        return

    await state.update_data(fio=value)

    await state.set_state(RegisterForm.phone1)
    await message.answer(
        "📱 Asosiy raqamni kiriting.\n\n"
        "Masalan: 901234567 yoki +998901234567",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(RegisterForm.phone1)
async def phone1_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)

    if not phone:
        await message.answer(
            "❌ Asosiy raqam noto‘g‘ri.\n"
            "Masalan: 901234567 yoki +998901234567"
        )
        return

    await state.update_data(phone1=phone)

    await state.set_state(RegisterForm.phone2)
    await message.answer(
        "📲 Qo'shimcha raqamni kiriting.\n\n"
        "Masalan: 901234567 yoki +998901234567"
    )


@dp.message(RegisterForm.phone2)
async def phone2_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)

    if not phone:
        await message.answer(
            "❌ Qo‘shimcha raqam noto‘g‘ri.\n"
            "Masalan: 901234567 yoki +998901234567"
        )
        return

    data = await state.get_data()

    row_data = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "telegram_user_id": message.from_user.id,
        "telegram_full_name": message.from_user.full_name,
        "telegram_username": message.from_user.username or "",
        "Ta'lim shakli": data["talim"],
        "Kurs": data["kurs"],
        "Guruh": data["guruh"],
        "F.I.SH.": data["fio"],
        "Asosiy raqam": data["phone1"],
        "Qo'shimcha raqam": phone,
    }

    save_result(row_data)
    await state.clear()

    await message.answer(
        "✅ Ma'lumotlaringiz muvaffaqiyatli saqlandi!\n\n"
        "📄 Ko‘rish: /myinfo\n"
        "✏️ Tahrirlash: /edit"
    )


# =========================
# TAHRIRLASH
# =========================
@dp.message(EditForm.phone1)
async def edit_phone1_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)

    if not phone:
        await message.answer(
            "❌ Asosiy raqam noto‘g‘ri.\n"
            "Masalan: 901234567 yoki +998901234567"
        )
        return

    await state.update_data(phone1=phone)
    await state.set_state(EditForm.phone2)

    await message.answer(
        "📲 Yangi qo‘shimcha raqamni kiriting:\n"
        "Masalan: 901234567 yoki +998901234567"
    )


@dp.message(EditForm.phone2)
async def edit_phone2_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)

    if not phone:
        await message.answer(
            "❌ Qo‘shimcha raqam noto‘g‘ri.\n"
            "Masalan: 901234567 yoki +998901234567"
        )
        return

    data = await state.get_data()
    ok = update_user_phones(
        telegram_user_id=message.from_user.id,
        main_phone=data["phone1"],
        extra_phone=phone,
    )

    await state.clear()

    if ok:
        await message.answer(
            "✅ Raqamlar muvaffaqiyatli yangilandi!\n\n📄 Ko‘rish: /myinfo"
        )
    else:
        await message.answer("❌ Tahrirlashda xatolik bo‘ldi.")


# =========================
# ADMIN BUYRUQLAR
# =========================
@dp.message(Command("stat"))
async def stat_handler(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Sizda bu buyruq uchun ruxsat yo'q.")
        return

    records = get_all_records()
    total = len(records)

    if total == 0:
        await message.answer("📊 Hozircha ma'lumot yo'q.")
        return

    df = pd.DataFrame(records)

    talim_stats = df["Ta'lim shakli"].value_counts().to_dict() if "Ta'lim shakli" in df else {}
    kurs_stats = df["Kurs"].value_counts().to_dict() if "Kurs" in df else {}

    text = [f"📊 Jami topshirilganlar: {total}\n"]

    text.append("🎓 Ta'lim shakli bo'yicha:")
    for k, v in talim_stats.items():
        text.append(f"• {k}: {v}")

    text.append("\n📚 Kurs bo'yicha:")
    for k, v in kurs_stats.items():
        text.append(f"• {k}: {v}")

    await message.answer("\n".join(text))


@dp.message(Command("excel"))
async def excel_handler(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Sizda bu buyruq uchun ruxsat yo'q.")
        return

    records = get_all_records()
    if not records:
        await message.answer("📄 Eksport qilish uchun ma'lumot yo'q.")
        return

    df = pd.DataFrame(records)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Telefonlar", index=False)

    output.seek(0)

    file = BufferedInputFile(
        output.read(),
        filename=f"telefonlar_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
    )

    await message.answer_document(file, caption="📥 Excel eksport tayyor")


async def main():
    ensure_sheet_header()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
