from aiogram.utils.keyboard import ReplyKeyboardBuilder


def menu_kb():
    kb = ReplyKeyboardBuilder()
    kb.button(text='🆕 Создать Маршрут(ы)')
    kb.button(text='🖎 Редактировать Маршрут')
    kb.button(text='❔Инструкция')
    kb.adjust(3)
    return kb.as_markup(resize_keyboard=True)
