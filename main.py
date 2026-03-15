import asyncio
import json
import logging
import os
import re
from datetime import datetime
from io import BytesIO
from pathlib import Path

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

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Talabalar Telefonlari").strip()
GOOGLE_WORKSHEET_NAME = os.getenv("GOOGLE_WORKSHEET_NAME", "Sheet1").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "").strip()
ADMIN_IDS = {
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
}

INPUT_FILE = "students.xlsx"

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi")

REQUIRED_COLUMNS = ["Ta'lim shakli", "Kurs", "Guruh", "F.I.SH."]


# =========================
# YORDAMCHI FUNKSIYALAR
# =========================
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


def normalize_phone(phone_text: str) -> str | None:
    phone = phone_text.strip()
    phone = re.sub(r"[^\d+]", "", phone)

    if re.fullmatch(r"\+998\d{9}", phone):
        return phone
    if re.fullmatch(r"998\d{9}", phone):
        return f"+{phone}"
    if re.fullmatch(r"\d{9}", phone):
        return f"+998{phone}"

    return None


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def load_students() -> pd.DataFrame:
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"{INPUT_FILE} topilmadi")

    df = pd.read_excel(INPUT_FILE)
    df.columns = [str(c).strip() for c in df.columns]

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"{INPUT_FILE} ichida '{col}' ustuni yo'q")

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
def get_gspread_client():
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        return gspread.service_account_from_dict(creds_dict)

    if GOOGLE_CREDENTIALS_FILE and Path(GOOGLE_CREDENTIALS_FILE).exists():
        return gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)

    json_files = list(Path(".").glob("*.json"))
    if json_files:
        return gspread.service_account(filename=str(json_files[0]))

    raise ValueError(
        "Google credentials topilmadi. GOOGLE_SERVICE_ACCOUNT_JSON yoki json fayl kerak."
    )


def get_worksheet():
    gc = get_gspread_client()
    sh = gc.open(GOOGLE_SHEET_NAME)
    ws = sh.worksheet(GOOGLE_WORKSHEET_NAME)
    return ws


def ensure_sheet_header():
    ws = get_worksheet()
    values = ws.get_all_values()

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
        ws.append_row(header)


def get_all_records():
    ws = get_worksheet()
    return ws.get_all_records()


def find_user_row(telegram_user_id: int):
    ws = get_worksheet()
    records = ws.get_all_records()

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
    ws = get_worksheet()
    ws.append_row(
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
    ws = get_worksheet()
    row_number, _ = find_user_row(telegram_user_id)

    if not row_number:
        return False

    # B = updated_at, J = Asosiy raqam, K = Qo'shimcha raqam
    ws.update_cell(row_number, 2, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    ws.update_cell(row_number, 10, main_phone)
    ws.update_cell(row_number, 11, extra_phone)
    return True


# =========================
# DATA
# =========================
students_df = load_students()


# =========================
# STATES
# =========================
class Form(StatesGroup):
    talim = State()
    kurs = State()
    guruh = State()
    fio = State()
    main_phone = State()
    extra_phone = State()


class EditForm(StatesGroup):
    main_phone = State()
    extra_phone = State()


# =========================
# BOT
# =========================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# =========================
# USER COMMANDS
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

    await state.set_state(Form.talim)
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
async def cancel_handler(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "❌ Jarayon bekor qilindi.\nQayta boshlash uchun /start bosing.",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(Command("myinfo"))
async def myinfo_handler(message: Message):
    _, row = find_user_row(message.from_user.id)

    if not row:
        await message.answer("ℹ️ Siz hali ma'lumot yubormagansiz. /start bosing.")
        return

    text = (
        "📄 Sizning saqlangan ma'lumotlaringiz:\n\n"
        f"🎓 Ta'lim shakli: {row.get(\"Ta'lim shakli\", '')}\n"
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
    await state.set_state(EditForm.main_phone)
    await message.answer(
        "✏️ Raqamlarni qayta tahrirlash boshlandi.\n\n"
        "📱 Yangi asosiy raqamni kiriting:\n"
        "Misol: 901234567 yoki +998901234567"
    )


# =========================
# RO‘YXATDAN O‘TISH
# =========================
@dp.message(Form.talim)
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

    await state.set_state(Form.kurs)
    await message.answer(
        "📚 Kursingizni tanlang:",
        reply_markup=make_keyboard(courses, row_width=3),
    )


@dp.message(Form.kurs)
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

    await state.set_state(Form.guruh)
    await message.answer(
        "👥 Guruhingizni tanlang:",
        reply_markup=make_keyboard(groups, row_width=3),
    )


@dp.message(Form.guruh)
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

    await state.set_state(Form.fio)
    await message.answer(
        "🧑‍🎓 F.I.SH. ni tanlang:",
        reply_markup=make_keyboard(names, row_width=1),
    )


@dp.message(Form.fio)
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
        await message.answer(
            "⚠️ Bu talaba uchun ma'lumot allaqachon kiritilgan."
        )
        return

    await state.update_data(fio=value)

    await state.set_state(Form.main_phone)
    await message.answer(
        "📱 Asosiy raqamni kiriting.\n\n"
        "Misol: 901234567 yoki +998901234567",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(Form.main_phone)
async def main_phone_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)

    if not phone:
        await message.answer(
            "❌ Asosiy raqam noto‘g‘ri.\n"
            "Misol: 901234567 yoki +998901234567"
        )
        return

    await state.update_data(main_phone=phone)

    await state.set_state(Form.extra_phone)
    await message.answer(
        "📲 Qo'shimcha raqamni kiriting.\n\n"
        "Misol: 901234567 yoki +998901234567"
    )


@dp.message(Form.extra_phone)
async def extra_phone_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)

    if not phone:
        await message.answer(
            "❌ Qo‘shimcha raqam noto‘g‘ri.\n"
            "Misol: 901234567 yoki +998901234567"
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
        "Asosiy raqam": data["main_phone"],
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
@dp.message(EditForm.main_phone)
async def edit_main_phone_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)

    if not phone:
        await message.answer(
            "❌ Asosiy raqam noto‘g‘ri.\n"
            "Misol: 901234567 yoki +998901234567"
        )
        return

    await state.update_data(main_phone=phone)
    await state.set_state(EditForm.extra_phone)

    await message.answer(
        "📲 Yangi qo‘shimcha raqamni kiriting:\n"
        "Misol: 901234567 yoki +998901234567"
    )


@dp.message(EditForm.extra_phone)
async def edit_extra_phone_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)

    if not phone:
        await message.answer(
            "❌ Qo‘shimcha raqam noto‘g‘ri.\n"
            "Misol: 901234567 yoki +998901234567"
        )
        return

    data = await state.get_data()
    ok = update_user_phones(
        telegram_user_id=message.from_user.id,
        main_phone=data["main_phone"],
        extra_phone=phone,
    )

    await state.clear()

    if ok:
        await message.answer(
            "✅ Raqamlar muvaffaqiyatli yangilandi!\n\n"
            "📄 Ko‘rish: /myinfo"
        )
    else:
        await message.answer("❌ Tahrirlashda xatolik bo‘ldi.")


# =========================
# ADMIN COMMANDS
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
