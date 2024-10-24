from aiogram.fsm.state import StatesGroup, State


class AllStates(StatesGroup):
    getSelect = State()
    selectAction = State()
    instruction = State()
    stepImportNew = State()
    stepExportNew = State()
    stepCreateNew = State()