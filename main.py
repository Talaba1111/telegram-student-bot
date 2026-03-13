from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import CommandStart
import asyncio

TOKEN = "8360783769:AAFDlJ2LXz6kbSMTAWags5K5fzkX-uEgzng"

bot = Bot(token=TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def start(message: Message):
    await message.answer("Bot ishlayapti!")

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())