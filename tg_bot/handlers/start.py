from aiogram import Bot
from aiogram.types import Message
from tg_bot.kb_constant import menu_kb


async def get_start(message: Message, bot: Bot):
    await bot.send_message(message.from_user.id, f'Привет, {message.from_user.first_name}',
                           reply_markup=menu_kb())
