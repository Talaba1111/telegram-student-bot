import asyncio
import json
import logging
import os
import re
from datetime import datetime

import pandas as pd
import gspread

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "").strip()
GOOGLE_WORKSHEET_NAME = os.getenv("GOOGLE_WORKSHEET_NAME", "Sheet1").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi")

if not GOOGLE_SHEET_NAME:
    raise ValueError("GOOGLE_SHEET_NAME topilmadi")

if not GOOGLE_SERVICE_ACCOUNT_JSON:
    raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON topilmadi")

INPUT_FILE = "students.xlsx"


def load_students():
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"{INPUT_FILE} topilmadi")

    df = pd.read_excel(INPUT_FILE)
    df.columns = [str(c).strip() for c in df.columns]

    required_cols = ["Ta'lim shakli", "Kurs", "Guruh", "F.I.SH."]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"{INPUT_FILE} ichida '{col}' ustuni yo'q")

    df = df[required_cols].copy().dropna()

    for col in required_cols:
        df[col] = df[col].astype(str).str.strip()

    # bo'sh satrlarni olib tashlash
    df = df[
        (df["Ta'lim shakli"] != "") &
        (df["Kurs"] != "") &
        (df["Guruh"] != "") &
        (df["F.I.SH."] != "")
    ].copy()

    return df


def get_gspread_client():
    creds = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    return gspread.service_account_from_dict(creds)


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
        "telegram_user_id",
        "telegram_username",
        "Ta'lim shakli",
        "Kurs",
        "Guruh",
        "F.I.SH.",
        "Asosiy raqam",
        "Qo'shimcha raqam",
        "Ota-onasining raqami",
    ]

    if not values:
        ws.append_row(header)


def save_result(data: dict):
    ws = get_worksheet()
    ws.append_row([
        data["created_at"],
        data["telegram_user_id"],
        data["telegram_username"],
        data["Ta'lim shakli"],
        data["Kurs"],
        data["Guruh"],
        data["F.I.SH."],
        data["Asosiy raqam"],
        data["Qo'shimcha raqam"],
        data["Ota-onasining raqami"],
    ])


def normalize_phone(phone_text: str):
    """
    9 xonali raqam kiritilsa avtomatik +998 qo'shiladi.
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
        rows.append([KeyboardButton(text="Bekor qilish")])

    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        one_time_keyboard=True
    )


students_df = load_students()


class Form(StatesGroup):
    talim = State()
    kurs = State()
    guruh = State()
    fio = State()
    choose_phone_action = State()
    enter_main_phone = State()
    enter_extra_phone = State()
    enter_parent_phone = State()


bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    await state.clear()

    talim_list = sorted(students_df["Ta'lim shakli"].unique().tolist())

    if not talim_list:
        await message.answer(
            "Talabalar ro'yxati hali kiritilmagan. Administrator students.xlsx faylni to'ldirishi kerak."
        )
        return

    await state.set_state(Form.talim)
    await message.answer(
        "Assalomu alaykum.\nTa'lim shaklini tanlang:",
        reply_markup=make_keyboard(talim_list, row_width=2)
    )


@dp.message(F.text == "Bekor qilish")
async def cancel_handler(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Jarayon bekor qilindi. Qayta boshlash uchun /start bosing.",
        reply_markup=ReplyKeyboardRemove()
    )


@dp.message(Form.talim)
async def talim_handler(message: Message, state: FSMContext):
    value = message.text.strip()
    talim_list = sorted(students_df["Ta'lim shakli"].unique().tolist())

    if value not in talim_list:
        await message.answer("Ro'yxatdan birini tanlang.")
        return

    await state.update_data(talim=value)

    courses = sorted(
        students_df[students_df["Ta'lim shakli"] == value]["Kurs"].unique().tolist(),
        key=lambda x: str(x)
    )

    await state.set_state(Form.kurs)
    await message.answer(
        "Kursni tanlang:",
        reply_markup=make_keyboard(courses, row_width=3)
    )


@dp.message(Form.kurs)
async def kurs_handler(message: Message, state: FSMContext):
    value = message.text.strip()
    data = await state.get_data()

    courses = sorted(
        students_df[students_df["Ta'lim shakli"] == data["talim"]]["Kurs"].unique().tolist(),
        key=lambda x: str(x)
    )
    courses = [str(x) for x in courses]

    if value not in courses:
        await message.answer("Ro'yxatdan birini tanlang.")
        return

    await state.update_data(kurs=value)

    groups = sorted(
        students_df[
            (students_df["Ta'lim shakli"] == data["talim"]) &
            (students_df["Kurs"].astype(str) == value)
        ]["Guruh"].unique().tolist()
    )

    await state.set_state(Form.guruh)
    await message.answer(
        "Guruhni tanlang:",
        reply_markup=make_keyboard(groups, row_width=3)
    )


@dp.message(Form.guruh)
async def guruh_handler(message: Message, state: FSMContext):
    value = message.text.strip()
    data = await state.get_data()

    groups = sorted(
        students_df[
            (students_df["Ta'lim shakli"] == data["talim"]) &
            (students_df["Kurs"].astype(str) == data["kurs"])
        ]["Guruh"].unique().tolist()
    )

    if value not in groups:
        await message.answer("Ro'yxatdan birini tanlang.")
        return

    await state.update_data(guruh=value)

    names = sorted(
        students_df[
            (students_df["Ta'lim shakli"] == data["talim"]) &
            (students_df["Kurs"].astype(str) == data["kurs"]) &
            (students_df["Guruh"] == value)
        ]["F.I.SH."].unique().tolist()
    )

    await state.set_state(Form.fio)
    await message.answer(
        "F.I.SH. ni tanlang:",
        reply_markup=make_keyboard(names, row_width=1)
    )


@dp.message(Form.fio)
async def fio_handler(message: Message, state: FSMContext):
    value = message.text.strip()
    data = await state.get_data()

    names = sorted(
        students_df[
            (students_df["Ta'lim shakli"] == data["talim"]) &
            (students_df["Kurs"].astype(str) == data["kurs"]) &
            (students_df["Guruh"] == data["guruh"])
        ]["F.I.SH."].unique().tolist()
    )

    if value not in names:
        await message.answer("Ro'yxatdan birini tanlang.")
        return

    await state.update_data(fio=value)

    await state.set_state(Form.choose_phone_action)
    await message.answer(
        "Raqamlarni kiriting.\n"
        "Avval Asosiy raqamni bosing.",
        reply_markup=make_keyboard(
            ["Asosiy raqam", "Qo'shimcha raqam", "Ota-onasining raqami"],
            row_width=1
        )
    )


@dp.message(Form.choose_phone_action)
async def choose_phone_action_handler(message: Message, state: FSMContext):
    action = message.text.strip()
    data = await state.get_data()

    allowed = ["Asosiy raqam", "Qo'shimcha raqam", "Ota-onasining raqami"]
    if action not in allowed:
        await message.answer("Ro'yxatdan birini tanlang.")
        return

    if action == "Asosiy raqam":
        await state.set_state(Form.enter_main_phone)
        await message.answer(
            "Asosiy raqamni kiriting.\nMasalan: 901234567 yoki +998901234567",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if action == "Qo'shimcha raqam":
        if not data.get("main_phone"):
            await message.answer("Avval Asosiy raqamni kiriting.")
            return

        await state.set_state(Form.enter_extra_phone)
        await message.answer(
            "Qo'shimcha raqamni kiriting.\nMasalan: 901234567 yoki +998901234567",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if action == "Ota-onasining raqami":
        if not data.get("main_phone") or not data.get("extra_phone"):
            await message.answer("Avval Asosiy va Qo'shimcha raqamni kiriting.")
            return

        await state.set_state(Form.enter_parent_phone)
        await message.answer(
            "Ota-onasining raqamini kiriting.\nKiritmasangiz O'tkazib yuborish ni bosing.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="O'tkazib yuborish")],
                    [KeyboardButton(text="Bekor qilish")]
                ],
                resize_keyboard=True
            )
        )


@dp.message(Form.enter_main_phone)
async def enter_main_phone_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)

    if not phone:
        await message.answer("Raqam noto'g'ri. Masalan: 901234567 yoki +998901234567")
        return

    await state.update_data(main_phone=phone)

    await state.set_state(Form.choose_phone_action)
    await message.answer(
        f"Asosiy raqam saqlandi: {phone}\nEndi Qo'shimcha raqamni kiriting.",
        reply_markup=make_keyboard(
            ["Asosiy raqam", "Qo'shimcha raqam", "Ota-onasining raqami"],
            row_width=1
        )
    )


@dp.message(Form.enter_extra_phone)
async def enter_extra_phone_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)

    if not phone:
        await message.answer("Raqam noto'g'ri. Masalan: 901234567 yoki +998901234567")
        return

    await state.update_data(extra_phone=phone)

    await state.set_state(Form.choose_phone_action)
    await message.answer(
        f"Qo'shimcha raqam saqlandi: {phone}\n"
        "Ota-onasining raqamini kiriting yoki O'tkazib yuborishingiz mumkin.",
        reply_markup=make_keyboard(
            ["Asosiy raqam", "Qo'shimcha raqam", "Ota-onasining raqami"],
            row_width=1
        )
    )


@dp.message(Form.enter_parent_phone, F.text == "O'tkazib yuborish")
async def skip_parent_phone_handler(message: Message, state: FSMContext):
    data = await state.get_data()

    row_data = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "telegram_user_id": message.from_user.id,
        "telegram_username": message.from_user.username or "",
        "Ta'lim shakli": data["talim"],
        "Kurs": data["kurs"],
        "Guruh": data["guruh"],
        "F.I.SH.": data["fio"],
        "Asosiy raqam": data["main_phone"],
        "Qo'shimcha raqam": data["extra_phone"],
        "Ota-onasining raqami": "",
    }

    save_result(row_data)
    await state.clear()

    await message.answer(
        "Ma'lumot muvaffaqiyatli saqlandi.",
        reply_markup=ReplyKeyboardRemove()
    )


@dp.message(Form.enter_parent_phone)
async def enter_parent_phone_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)

    if not phone:
        await message.answer("Raqam noto'g'ri. Masalan: 901234567 yoki +998901234567")
        return

    data = await state.get_data()

    row_data = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "telegram_user_id": message.from_user.id,
        "telegram_username": message.from_user.username or "",
        "Ta'lim shakli": data["talim"],
        "Kurs": data["kurs"],
        "Guruh": data["guruh"],
        "F.I.SH.": data["fio"],
        "Asosiy raqam": data["main_phone"],
        "Qo'shimcha raqam": data["extra_phone"],
        "Ota-onasining raqami": phone,
    }

    save_result(row_data)
    await state.clear()

    await message.answer(
        "Ma'lumot muvaffaqiyatli saqlandi.",
        reply_markup=ReplyKeyboardRemove()
    )


async def main():
    ensure_sheet_header()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
