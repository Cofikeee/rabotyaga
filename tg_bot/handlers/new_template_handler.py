import pandas as pd
import uuid
from aiogram import Bot
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from tg_bot.state import AllStates
from tg_bot.kb_constant import menu_kb
from utils import unload_sql_tg, add_id
from _worker.main_compiler import main as main_compiler


async def edit_new(message: Message, state: FSMContext):
    global edit_route_id
    edit_route_id = message.text
    # Новый запрос на создание маршрута документа
    await message.answer(f'Нужно указать uuid маршрута из таблицы signing_route_template')
    await state.set_state(AllStates.stepCreateNew)


async def create_new(message: Message, state: FSMContext):
    global file_uid_list, file_counter, missing_values, edit_route_id

    # Новый запрос на создание маршрута документа
    missing_values, file_uid_list = [], []
    file_counter = 0
    uuid_check = ''

    try:
        edit_route_id = message.text
        uuid.UUID(edit_route_id)
    except (TypeError, ValueError):
        uuid_check = 'failed'
        if message.text == '🆕 Создать Маршрут(ы)':
            await message.answer(f'Нужно прикрепить xlsx файл')
            await state.set_state(AllStates.stepExportNew)
        else:
            await message.answer(f'Указан невалидный uuid')
            await message.answer(f'Нужно указать uuid маршрута из таблицы signing_route_template')
            await state.set_state(AllStates.stepCreateNew)

    except NameError:
        edit_route_id = None
        await message.answer(f'Нужно прикрепить xlsx файл')
        await state.set_state(AllStates.stepExportNew)

    if uuid_check != 'failed':
        await message.answer(f'Нужно прикрепить xlsx файл')
        await state.set_state(AllStates.stepExportNew)


async def export_new(message: Message, state: FSMContext, bot: Bot):
    global file_uid_list, missing_values, file_counter, edit_route_id

    # Сбор информации о файле
    file_id = message.document
    file_uid = file_id.file_unique_id
    file_uid_list.append(file_uid)

    # Загрузка файла
    await bot.download(file_id, f'_worker/data/in/{file_uid}.xlsx', seek=False)

    # Счётчик файлов
    file_counter += 1

    try:
        # Проверка, открывается ли файл
        pd.DataFrame(pd.read_excel(open(f'_worker/data/in/{file_uid}.xlsx', 'rb'), engine='openpyxl'))

    except FileNotFoundError:
        await bot.send_message(message.chat.id, '\nФайл был сохранен нечитаемом для меня формате (возможно xls)'
                                                '\nПопробуй открыть в Google Sheets и загрузить оттуда в xlsx .',
                               reply_markup=menu_kb())
        await state.set_state(AllStates.stepExportNew)
        return

    except (TypeError, IndexError):
        await bot.send_message(message.chat.id, '\nФайл был сохранен в нечитаемом для меня формате.'
                                                '\nПопробуй открыть в Google Sheets и загрузить оттуда.',
                               reply_markup=menu_kb())
        await state.set_state(AllStates.stepExportNew)
        return

    # Сбор ошибок и полезной информации о скрипте
    alerts = main_compiler(file_uid, edit_route_id)
    for alert in alerts:
        alert = ['```Alert\n' + alert + '\n```']
        missing_values.append(alert)

    # Проверка количества прикрепленных файлов при редактировании
    if 1 < len(file_uid_list) == file_counter and edit_route_id is not None:
        file_counter = 0
        await bot.send_message(message.from_user.id, 'Прикреплено более 1 файла',
                               reply_markup=menu_kb())
        await state.set_state(AllStates.stepExportNew)

    # Проверка количества обработанных файлов
    if file_counter == len(file_uid_list):
        # Создание записи group_id и uid файлов в бд
        add_id(file_uid_list)

        # Компайл sql-скрипта и pretty-превью
        sql_query, pretty_output = unload_sql_tg(file_uid_list, edit_route_id)

        # Выгрузка sql-скрипта
        await bot.send_document(message.chat.id, sql_query)

        # Выгрузка pretty-превью
        await bot.send_document(message.chat.id, pretty_output)

        # Выгрузка ошибок и полезной информации о скрипте
        for missing_value in missing_values:
            for i in missing_value:
                await bot.send_message(message.from_user.id, i,
                                       parse_mode="MarkdownV2",
                                       reply_markup=menu_kb())
