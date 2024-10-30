import asyncio
import logging
import os
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.client.bot import DefaultBotProperties
from aiogram.enums.parse_mode import ParseMode

from tg_bot.state import AllStates
from tg_bot.handlers.start import get_start
from tg_bot.handlers.instruction import instruction
from tg_bot.handlers.new_template_handler import export_new, create_new, edit_new

load_dotenv()
token = os.getenv('TEST_BOT_TOKEN')
#token = os.getenv('BOT_TOKEN')
admin_id = os.getenv('ADMIN_ID')
bot = Bot(token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()


async def start():

    # Игнорируем сообщения пришедшие в оффлайн
    await bot.delete_webhook(drop_pending_updates=True)

    # Запускаем логгер
    logging.basicConfig(#filename='files/log.log',
                        #filemode='a',
                        level=logging.INFO,
                        format='%(asctime)s - [%(levelname)s] - %(name)s - '
                               '(%(filename)s).%(funcName)s(%(lineno)d) - %(message)s')

    # Команда старт
    dp.message.register(get_start, Command(commands='start'))

    # Хэндлеры статичного меню
    dp.message.register(create_new, F.text.contains('Создать'))
    dp.message.register(edit_new, F.text.contains('Редактировать'))
    dp.message.register(instruction, F.text == '❔Инструкция')

    # Хэндлер создания нового маршрута
    dp.message.register(create_new, AllStates.stepCreateNew)
    dp.message.register(export_new, AllStates.stepExportNew)

    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await bot.session.close()


if __name__ == '__main__':
    asyncio.run(start())
