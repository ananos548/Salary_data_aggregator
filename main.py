from datetime import timedelta

from aiogram import Bot, Dispatcher, types
from datetime import datetime as iso
import json
import asyncio

from service import aggregator
from aiogram.filters.command import Command
from config import TOKEN

bot = Bot(token=TOKEN)
dp = Dispatcher()


@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer("Привет! Чтобы получить агрегированные данные, отправь JSON с входными данными.")


@dp.message(lambda message: message.text.startswith("{"))
async def process_json_message(message: types.Message):
    try:
        data = json.loads(message.text)
        dt_from = iso.fromisoformat(data["dt_from"])
        dt_upto = iso.fromisoformat(data["dt_upto"])
        group_type = data["group_type"]

        result = await aggregator(dt_from, dt_upto, group_type)
        if result:
            await message.answer(json.dumps({"dataset": result['dataset'], "labels": result['labels']}))
        else:
            await message.answer("Некорректный тип агрегации. Поддерживаемые типы: hour, day, month")

    except Exception as e:
        await message.answer(f"{str(e)}")


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
