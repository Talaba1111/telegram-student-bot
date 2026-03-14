import asyncio
import logging
import os
import re
from datetime import datetime

import pandas as pd
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup, ReplyKeyboardRemove

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi")

INPUT_FILE = "students.xlsx"
OUTPUT_FILE = "natijalar.xlsx"


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
        rows.append([KeyboardButton(text="Bekor qilish")])

    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        one_time_keyboard=True
    )


def normalize_phone(phone_text: str):
    """
    Foydalanuvchi 9 ta raqam kiritsa -> +998 qo‘shiladi
    To‘liq +998... yozsa ham qabul qiladi
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


def load_students():
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"{INPUT_FILE} topilmadi")

    df = pd.read_excel(INPUT_FILE)
    df.columns = [str(c).strip() for c in df.columns]

    required = ["Talim_shakli", "Kurs", "Guruh", "Ism_familiya"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"{INPUT_FILE} ichida '{col}' ustuni yo'q")

    df = df[required].copy().dropna()
    for col in required:
        df[col] = df[col].astype(str).str.strip()

    return df


def ensure_output_file():
    if not os.path.exists(OUTPUT_FILE):
        df = pd.DataFrame(columns=[
            "Sana",
            "Telegram_ID",
            "Telegram_username",
            "Talim_shakli",
            "Kurs",
            "Guruh",
            "Ism_familiya",
            "Asosiy_raqam",
            "Qoshimcha_raqam",
            "Ota_ona_raqami",
        ])
        df.to_excel(OUTPUT_FILE, index=False)


def save_result(data: dict):
    ensure_output_file()
    df = pd.read_excel(OUTPUT_FILE)

    new_row = pd.DataFrame([data])
    df = pd.concat([df, new_row], ignore_index=True)
    df.to_excel(OUTPUT_FILE, index=False)


students_df = load_students()


# =========================
# STATE
# =========================
class Form(StatesGroup):
    talim = State()
    kurs = State()
    guruh = State()
    fio = State()
    choose_phone_action = State()
    enter_main_phone = State()
    enter_extra_phone = State()
    enter_parent_phone = State()


# =========================
# BOT
# =========================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# =========================
# START
# =========================
@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    await state.clear()

    talim_list = sorted(students_df["Talim_shakli"].unique().tolist())

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


# =========================
# TALIM SHAKLI
# =========================
@dp.message(Form.talim)
async def talim_handler(message: Message, state: FSMContext):
    value = message.text.strip()
    talim_list = sorted(students_df["Talim_shakli"].unique().tolist())

    if value not in talim_list:
        await message.answer("Ro'yxatdan birini tanlang.")
        return

    await state.update_data(Talim_shakli=value)

    courses = sorted(
        students_df[students_df["Talim_shakli"] == value]["Kurs"].unique().tolist(),
        key=lambda x: str(x)
    )

    await state.set_state(Form.kurs)
    await message.answer(
        "Kursni tanlang:",
        reply_markup=make_keyboard(courses, row_width=3)
    )


# =========================
# KURS
# =========================
@dp.message(Form.kurs)
async def kurs_handler(message: Message, state: FSMContext):
    value = message.text.strip()
    data = await state.get_data()

    courses = sorted(
        students_df[students_df["Talim_shakli"] == data["Talim_shakli"]]["Kurs"].unique().tolist(),
        key=lambda x: str(x)
    )
    courses = [str(x) for x in courses]

    if value not in courses:
        await message.answer("Ro'yxatdan birini tanlang.")
        return

    await state.update_data(Kurs=value)

    groups = sorted(
        students_df[
            (students_df["Talim_shakli"] == data["Talim_shakli"]) &
            (students_df["Kurs"].astype(str) == value)
        ]["Guruh"].unique().tolist()
    )

    await state.set_state(Form.guruh)
    await message.answer(
        "Guruhni tanlang:",
        reply_markup=make_keyboard(groups, row_width=3)
    )


# =========================
# GURUH
# =========================
@dp.message(Form.guruh)
async def guruh_handler(message: Message, state: FSMContext):
    value = message.text.strip()
    data = await state.get_data()

    groups = sorted(
        students_df[
            (students_df["Talim_shakli"] == data["Talim_shakli"]) &
            (students_df["Kurs"].astype(str) == data["Kurs"])
        ]["Guruh"].unique().tolist()
    )

    if value not in groups:
        await message.answer("Ro'yxatdan birini tanlang.")
        return

    await state.update_data(Guruh=value)

    names = sorted(
        students_df[
            (students_df["Talim_shakli"] == data["Talim_shakli"]) &
            (students_df["Kurs"].astype(str) == data["Kurs"]) &
            (students_df["Guruh"] == value)
        ]["Ism_familiya"].unique().tolist()
    )

    await state.set_state(Form.fio)
    await message.answer(
        "Ism-familiyani tanlang:",
        reply_markup=make_keyboard(names, row_width=1)
    )


# =========================
# F.I.O.
# =========================
@dp.message(Form.fio)
async def fio_handler(message: Message, state: FSMContext):
    value = message.text.strip()
    data = await state.get_data()

    names = sorted(
        students_df[
            (students_df["Talim_shakli"] == data["Talim_shakli"]) &
            (students_df["Kurs"].astype(str) == data["Kurs"]) &
            (students_df["Guruh"] == data["Guruh"])
        ]["Ism_familiya"].unique().tolist()
    )

    if value not in names:
        await message.answer("Ro'yxatdan birini tanlang.")
        return

    await state.update_data(Ism_familiya=value)

    await state.set_state(Form.choose_phone_action)
    await message.answer(
        "Endi quyidagi tugmalar orqali raqamlarni kiriting.\n"
        "1) Asosiy raqam\n"
        "2) Qo'shimcha raqam\n"
        "3) Ota-onasining raqami (ixtiyoriy)\n\n"
        "Avval Asosiy raqamni bosing.",
        reply_markup=make_keyboard(
            ["Asosiy raqam", "Qo'shimcha raqam", "Ota-onasining raqami"],
            row_width=1
        )
    )


# =========================
# PHONE ACTION TANLASH
# =========================
@dp.message(Form.choose_phone_action)
async def phone_action_handler(message: Message, state: FSMContext):
    action = message.text.strip()
    data = await state.get_data()

    allowed = ["Asosiy raqam", "Qo'shimcha raqam", "Ota-onasining raqami"]
    if action not in allowed:
        await message.answer("Ro'yxatdan birini tanlang.")
        return

    if action == "Asosiy raqam":
        await state.set_state(Form.enter_main_phone)
        await message.answer(
            "Asosiy raqamni kiriting.\n"
            "Masalan: 901234567 yoki +998901234567",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if action == "Qo'shimcha raqam":
        if not data.get("Asosiy_raqam"):
            await message.answer("Avval Asosiy raqamni kiriting.")
            return

        await state.set_state(Form.enter_extra_phone)
        await message.answer(
            "Qo'shimcha raqamni kiriting.\n"
            "Masalan: 901234567 yoki +998901234567",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if action == "Ota-onasining raqami":
        if not data.get("Asosiy_raqam") or not data.get("Qoshimcha_raqam"):
            await message.answer("Avval Asosiy va Qo'shimcha raqamni kiriting.")
            return

        await state.set_state(Form.enter_parent_phone)
        await message.answer(
            "Ota-onasining raqamini kiriting.\n"
            "Agar kiritmoqchi bo'lmasangiz: O'tkazib yuborish tugmasini bosing.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="O'tkazib yuborish")],
                    [KeyboardButton(text="Bekor qilish")]
                ],
                resize_keyboard=True
            )
        )


# =========================
# ASOSIY RAQAM
# =========================
@dp.message(Form.enter_main_phone)
async def main_phone_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)

    if not phone:
        await message.answer("Raqam noto'g'ri. Masalan: 901234567 yoki +998901234567")
        return

    await state.update_data(Asosiy_raqam=phone)

    await state.set_state(Form.choose_phone_action)
    await message.answer(
        f"Asosiy raqam saqlandi: {phone}\n"
        "Endi Qo'shimcha raqamni bosing.",
        reply_markup=make_keyboard(
            ["Asosiy raqam", "Qo'shimcha raqam", "Ota-onasining raqami"],
            row_width=1
        )
    )


# =========================
# QO‘SHIMCHA RAQAM
# =========================
@dp.message(Form.enter_extra_phone)
async def extra_phone_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)

    if not phone:
        await message.answer("Raqam noto'g'ri. Masalan: 901234567 yoki +998901234567")
        return

    await state.update_data(Qoshimcha_raqam=phone)

    await state.set_state(Form.choose_phone_action)
    await message.answer(
        f"Qo'shimcha raqam saqlandi: {phone}\n"
        "Endi Ota-onasining raqamini kiriting yoki O'tkazib yuborishingiz mumkin.",
        reply_markup=make_keyboard(
            ["Asosiy raqam", "Qo'shimcha raqam", "Ota-onasining raqami"],
            row_width=1
        )
    )


# =========================
# OTA-ONA RAQAMI
# =========================
@dp.message(Form.enter_parent_phone, F.text == "O'tkazib yuborish")
async def skip_parent_phone(message: Message, state: FSMContext):
    data = await state.get_data()

    row = {
        "Sana": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Telegram_ID": message.from_user.id,
        "Telegram_username": message.from_user.username or "",
        "Talim_shakli": data["Talim_shakli"],
        "Kurs": data["Kurs"],
        "Guruh": data["Guruh"],
        "Ism_familiya": data["Ism_familiya"],
        "Asosiy_raqam": data["Asosiy_raqam"],
        "Qoshimcha_raqam": data["Qoshimcha_raqam"],
        "Ota_ona_raqami": "",
    }

    save_result(row)
    await state.clear()

    await message.answer(
        "Ma'lumot muvaffaqiyatli saqlandi.",
        reply_markup=ReplyKeyboardRemove()
    )


@dp.message(Form.enter_parent_phone)
async def parent_phone_handler(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)

    if not phone:
        await message.answer("Raqam noto'g'ri. Masalan: 901234567 yoki +998901234567")
        return

    data = await state.get_data()

    row = {
        "Sana": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Telegram_ID": message.from_user.id,
        "Telegram_username": message.from_user.username or "",
        "Talim_shakli": data["Talim_shakli"],
        "Kurs": data["Kurs"],
        "Guruh": data["Guruh"],
        "Ism_familiya": data["Ism_familiya"],
        "Asosiy_raqam": data["Asosiy_raqam"],
        "Qoshimcha_raqam": data["Qoshimcha_raqam"],
        "Ota_ona_raqami": phone,
    }

    save_result(row)
    await state.clear()

    await message.answer(
        "Ma'lumot muvaffaqiyatli saqlandi.",
        reply_markup=ReplyKeyboardRemove()
    )


# =========================
# MAIN
# =========================
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
