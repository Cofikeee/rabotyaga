from aiogram.types import Message
from aiogram import Bot
from tg_bot.kb_constant import menu_kb
from aiogram.types import FSInputFile


async def instruction(message: Message, bot: Bot):
    # Отправка инструкции в чат телеги
    instruction_pic = FSInputFile('files/instruction.png')
    await bot.send_photo(message.chat.id, photo=instruction_pic,
                         reply_markup=menu_kb())
