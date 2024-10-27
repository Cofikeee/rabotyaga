import numpy as np
import pandas as pd
import uuid
from tabulate import tabulate
from utils import get_current_time
from pathlib import Path


class File:
    def __init__(self,
                 file_uid: str = None,
                 file_type: str = None,
                 step: str = None,
                 df: pd.DataFrame = None) -> None:

        self.file_type = file_type
        self.file_uid = file_uid
        self.step = step
        self.df = df
        self.path_excel = f'_worker/data/in/{file_uid}.xlsx'
        self.path_df_to_csv = f'_worker/data/out/{file_uid}/_{file_type}_{step}_{file_uid}.csv'

    # Создание директории для csv файлов
    def create_output_dir(self) -> None:
        Path(f'_worker/data/out/{self.file_uid}').mkdir(parents=True, exist_ok=True)
        return

    # Получение типа маршрута
    def get_route_type(self) -> str:
        excel_path = f'_worker/data/in/{self.file_uid}.xlsx'
        df = pd.DataFrame(pd.read_excel(excel_path))
        if 'Привязка к юрлицу' in df.iloc[:, 0].to_list():
            return 'DOC'
        else:
            return 'APP'

    # Получение пустой основы для дф
    def get_empty_df(self) -> pd.DataFrame:
        return pd.read_csv(f'files/{self.step}.csv')

    # Переименование колонок согласно бд
    def update_columns(self) -> pd.DataFrame:
        if self.file_type == 'DOC':
            self.df.columns = ['stage_type',
                               'participant_type',
                               'link_to_fixed_employee',
                               'participant_action_type',
                               'stage_completeness_condition',
                               'participant_signing_type']
        else:
            self.df.columns = ['stage_type',
                               'participant_type',
                               'required',
                               'link_to_fixed_employee',
                               'related_participant_id',
                               'participant_action_type',
                               'stage_completeness_condition',
                               'can_delete_before_stage_completed',
                               'responsible_enabled',
                               'include_to_print_form_stamp',
                               'unchangeable']
        df = self.df.dropna(how='all')
        return df

    # Автоматическое определение ролей кадровик + ответственный
    def update_responsible_enabled(self) -> pd.DataFrame:
        df = self.df.assign(responsible_enabled='false')
        if 'ответственный' in df['participant_type'].to_list():
            if 'кадровики' in df['participant_type'].to_list():
                df.loc[df['participant_type'] == 'кадровики', 'responsible_enabled'] = 'true'
                df.loc[df['participant_type'] == 'ответственный', 'responsible_enabled'] = 'true'
                re_index = int(df[df['participant_type'] == 'ответственный'].index.to_series().to_string(index=False))
                ro_index = int(df[df['participant_type'] == 'кадровики'].index.to_series().to_string(index=False))
                if re_index < ro_index:
                    df.loc[re_index, 'participant_type'] = 'кадровики'
                    df.loc[ro_index, 'participant_type'] = 'ответственный'
            else:
                df.loc[df['participant_type'] == 'ответственный', 'participant_type'] = 'кадровики'
        return df

    # Создание основы дф + фикс колонок
    def formatted_data(self) -> pd.DataFrame:
        file = File(file_uid=self.file_uid, file_type=self.file_type, step=self.step)
        file.df = file.read_file(skip=True)
        file.df = file.update_columns()
        df = file.update_responsible_enabled()
        return df.reset_index(drop=True)

    def df_strip(self) -> pd.DataFrame:
        df = self.df
        if self.file_type == 'APP':
            old_range = list(range(1, 11))
            updated_range = list(range(0, 10))
        else:
            old_range = list(range(1, 6))
            updated_range = list(range(0, 5))

        # strip + lowercase + range update колонок для удобства
        temp_df = df.copy().iloc[:, old_range]
        df_obj = temp_df.select_dtypes('object')
        temp_df.loc[:, df_obj.columns] = df_obj.apply(lambda x: x.str.strip().str.lower())
        df.iloc[:, old_range] = temp_df.iloc[:, updated_range]
        with pd.option_context("future.no_silent_downcasting", True):
            df = df.replace(r'\n', ' ', regex=True).replace(r'^\s*$', np.NaN, regex=True)
        return df

    def read_file(self, skip: bool) -> pd.DataFrame:
        # Предварительная выгрузка, чтобы проверить сколько полей отсекать
        file = File(file_uid=self.file_uid)
        excel_path = file.path_excel
        excel_df = pd.read_excel(excel_path, engine='openpyxl')
        df = pd.DataFrame(excel_df)

        # Сколько rows пропускать
        if skip is True:
            sr = df[df.iloc[:, 0] == 'Название участника'].index[0] + 1
        else:
            sr = 0

        # Какие columns добавить
        if self.file_type == 'DOC':
            usecols = list(range(0, 6))
            df = File.read_excel(excel_path, sr, usecols)

        elif Participant.unchangeable in df.iloc[:, -1].to_list():
            usecols = list(range(0, 11))
            df = File.read_excel(excel_path, sr, usecols)

        elif Participant.required in df.iloc[:, 2].to_list():
            usecols = list(range(0, 10))
            df = File.read_excel(excel_path, sr, usecols)
            df.insert(10, Participant.unchangeable, np.NaN)

        elif Participant.include_to_print_form_stamp in df.iloc[:, -1].to_list():
            usecols = list(range(0, 9))
            df = File.read_excel(excel_path, sr, usecols)
            df.insert(2, Participant.required, np.NaN)
            df.insert(10, Participant.unchangeable, np.NaN)
        else:
            usecols = list(range(0, 8))
            df = File.read_excel(excel_path, sr, usecols)
            df.insert(2, Participant.required, np.NaN)
            df.insert(9, Participant.include_to_print_form_stamp, np.NaN)
            df.insert(10, Participant.unchangeable, np.NaN)

        file.df = df
        df = file.df_strip()
        return df

    def csv_printer(self) -> None:
        file = File(file_uid=self.file_uid, file_type=self.file_type, step=self.step)
        self.df.to_csv(file.path_df_to_csv, sep=',', index=False)
        return

    @staticmethod
    def read_excel(excel_path, sr, usecols):
        df = pd.DataFrame(pd.read_excel(excel_path, skiprows=sr, usecols=usecols))
        return df

    def update_file_type(self) -> str:
        pass

    def update_template_name(self) -> str:
        pass

    def get_template_id(self) -> str:
        pass


class Alert:
    def __init__(self):
        pass

    @staticmethod
    def check_template_name(template_name):
        if pd.isnull(template_name):
            return [f'❗ Отсутствует название у одного из маршрутов']
        else:
            return []


class Template(File):
    def __init__(self, file_uid, file_type):
        super().__init__(file_uid, file_type)
        self.step = 'template'

    def update_file_type(self) -> str:
        if self.file_type == 'DOC':
            file_type_name = 'DOCUMENT'

        else:
            file_type_name = 'APPLICATION'
        return file_type_name

    def update_template_name(self) -> str:
        # Получение названия маршрута
        file = File(file_uid=self.file_uid)
        excel_file = file.path_excel
        df_name = pd.DataFrame(pd.read_excel(excel_file))

        try:
            template_name = df_name.iloc[0, 1].strip()
        except AttributeError:
            template_name = df_name.iloc[0, 1]

        return template_name

    def get_template_id(self) -> str:
        file = File(file_uid=self.file_uid, file_type=self.file_type, step=self.step)
        template_df = pd.read_csv(file.path_df_to_csv)
        template_id = template_df.iloc[0, 0]
        return template_id


class Stage:
    def __init__(self, df=None):
        self.df = df

    @staticmethod
    def update_stage_completeness_condition(df):
        df.loc[df['stage_completeness_condition'] == 'один из участников совершил действие', 'stage_completeness_condition'] = 'ANY'
        df.loc[df['stage_completeness_condition'] == 'все участники совершили действие', 'stage_completeness_condition'] = 'ALL'
        df.loc[df['stage_completeness_condition'] == 'один из участников подписал/согласовал', 'stage_completeness_condition'] = 'ANY'
        df.loc[df['stage_completeness_condition'] == 'все участники подписали/согласовали', 'stage_completeness_condition'] = 'ALL'
        df.loc[df['stage_completeness_condition'].isna(), 'stage_completeness_condition'] = 'ALL'

    @staticmethod
    def update_can_delete_before_stage_completed(df):
        df.loc[df['can_delete_before_stage_completed'] == 'да', 'can_delete_before_stage_completed'] = 'true'
        df.loc[df['can_delete_before_stage_completed'] == 'нет', 'can_delete_before_stage_completed'] = np.NaN

    @staticmethod
    def stages_drop_duplicates(stages_df):
        print(stages_df)
        print(stages_df.to_string(index=False))
        subset = stages_df.columns[1::]
        with pd.option_context("future.no_silent_downcasting", True):
            stages_df = (stages_df.groupby("index_number", as_index=False)
                         .apply(lambda s: s.ffill().bfill())
                         .drop_duplicates(subset=subset)
                         .reset_index(drop=True))

        return stages_df

    @staticmethod
    def stages_receiver_fill(stages_df, template_id, file_type):
        index_number = stages_df['index_number'].iloc[len(stages_df) - 1]

        if file_type == 'DOC':
            filler = 'null'
        else:
            filler = 'false'
        stages_df.loc[len(stages_df.index)] = ([uuid.uuid4()] +
                                               [template_id] +
                                               [index_number + 1] +
                                               ['RECEIVING'] +
                                               ['null'] +
                                               [filler] +
                                               [filler])
        return stages_df

    @staticmethod
    def stages_fix_can_delete(stages_df):
        checker_df = stages_df[stages_df['stage_type'] != 'RECEIVING'].copy()
        checker_df['count'] = stages_df.groupby(['can_delete_before_stage_completed'])['id'].transform('count')
        checker_df = checker_df[['can_delete_before_stage_completed', 'count']]
        try:
            checker = checker_df['count'][checker_df['can_delete_before_stage_completed'] == 'true'].to_list()[0]
            if checker > 1:
                last_cd_mention = max(
                    stages_df.index[stages_df['can_delete_before_stage_completed'] == 'true'].tolist())
                stages_df = stages_df.assign(can_delete_before_stage_completed='false')
                stages_df._set_value(last_cd_mention, 'can_delete_before_stage_completed', 'true')
        except IndexError:
            stages_df._set_value(1, 'can_delete_before_stage_completed', 'true')
            pass
        stages_df.loc[stages_df['can_delete_before_stage_completed'].isna(), 'can_delete_before_stage_completed'] = 'false'
        return stages_df

    @staticmethod
    def stages_fill_app(df, stages_df, template_id):
        counter = 0
        employee_counter = 0
        stage_counter = 0
        condition = 'ANY'
        responsible_enabled = 'false'
        stages_df.loc[len(stages_df)] = ([uuid.uuid4()] +
                                         [template_id] +
                                         [0] +
                                         ['SIGNING'] +
                                         ['ALL'] +
                                         ['false'] +
                                         ['false'])

        for stage in df['stage_type'].fillna('null'):
            if str(stage).strip()[:6] == 'Этап №':
                counter += 1
                stage_counter += 1
                employee_counter = 0
            else:
                try:
                    if df['link_to_fixed_employee'][counter] == 'финальный этап получатели' \
                            or df['participant_type'][counter + 1] == 'фиксированные получатели' \
                            or df['participant_type'][counter + 1] == 'выбираемые получатели':
                        return stages_df
                    else:
                        can_cancel = df['can_delete_before_stage_completed'][counter]
                        if employee_counter == 0:
                            condition = df['stage_completeness_condition'][counter]
                            responsible_enabled = df['responsible_enabled'][counter]
                        else:
                            pass
                        stages_df.loc[counter] = ([uuid.uuid4()] +
                                                  [template_id] +
                                                  [stage_counter] +
                                                  ['SIGNING'] +
                                                  [condition] +
                                                  [responsible_enabled] +
                                                  [can_cancel])
                        counter += 1
                        employee_counter += 1
                except IndexError:
                    return stages_df
        return stages_df

    @staticmethod
    def stages_fill_doc(df, stages_df, template_id):
        counter = 0
        employee_counter = 0
        stage_counter = 0
        condition = 'ANY'
        for stage in df['stage_type'].fillna('null'):
            if str(stage)[:6] == 'Этап №':
                counter += 1
                stage_counter += 1
                employee_counter = 0
            else:
                if employee_counter == 0:
                    condition = [df['stage_completeness_condition'][counter]]
                else:
                    pass
                stages_df.loc[counter] = ([uuid.uuid4()] +
                                          [template_id] +
                                          [stage_counter] +
                                          ['SIGNING'] +
                                          condition +
                                          ['null'] +
                                          ['null'])
                counter += 1
                employee_counter += 1

        return stages_df

    @staticmethod
    def stages_fill(file_uid, file_type, template_id):
        # Создание пустого датафрейма stages
        file = File(file_uid=file_uid, file_type=file_type, step='stage')
        stages_df = file.get_empty_df()
        # Подружаем пустой
        df = file.formatted_data()

        if file_type == 'APP':
            stages_df = Stage.stages_fill_app(df, stages_df, template_id)
        else:
            stages_df = Stage.stages_fill_doc(df, stages_df, template_id)
        print('stages_df2', stages_df.to_string())
        return stages_df


class Participant:
    def __init__(self, df=None, receiver=None):
        self.receiver = receiver
        if self.receiver is None:
            self.df = df
        elif self.receiver is True:
            self.df = df[df['participant_action_type'] == 'RECEIVING']
        elif self.receiver is False:
            self.df = df[df['participant_action_type'] != 'RECEIVING']

    stage_type = 'Действие'
    participant_type = 'Тип участника'
    required = 'Обязательный/Необязательный'
    employee_id = 'Ссылка в браузере на карточку фиксированного сотрудника'
    participant_action_type = 'Действие'
    stage_completeness_condition = 'Условие завершения Этапа'
    participant_signing_type = 'Вид ЭП'
    related_participant_id = 'Этап, относительно которого определяется Руководитель отдела'
    can_delete_before_stage_completed = 'Можно отозвать заявление'
    responsible_enabled = 'Разрешить определять ответственного'
    include_to_print_form_stamp = 'Включать действие Участника в оттиск Заявления'
    unchangeable = 'Запрещено изменять автоматически подставленного участника'

    @staticmethod
    def update_participant_type(df):
        df.loc[df['participant_type'] == 'указывает заявитель', 'participant_type'] = 'SELECTABLE_EMPLOYEE'
        df.loc[df['participant_type'] == 'выбираемый', 'participant_type'] = 'SELECTABLE_EMPLOYEE'
        df.loc[df['participant_type'] == 'фиксированный', 'participant_type'] = 'FIXED_EMPLOYEE'
        df.loc[df['participant_type'] == 'кадровики', 'participant_type'] = 'ROLE'
        df.loc[df['participant_type'] == 'кадровик', 'participant_type'] = 'ROLE'
        df.loc[df['participant_type'] == 'ответственный', 'participant_type'] = 'RESPONSIBLE'
        df.loc[df['participant_type'] == 'сотрудник', 'participant_type'] = 'EMPLOYEE'
        df.loc[df['participant_type'] == 'руководитель (представитель юл)', 'participant_type'] = 'EMPLOYER'
        df.loc[df['participant_type'] == 'произвольное количество', 'multiple_signers'] = 'true'
        df.loc[df['participant_type'] == 'ROLE', 'docflow_system_role_id'] = '21c4a2d9-2451-41b1-84e3-cc0b7ad73277'
        df.loc[df['employee_id'] != 'null', 'participant_type'] = 'FIXED_EMPLOYEE'

        df.loc[df['participant_type'] == 'руководитель отдела', 'auto_select_rule_type'] = 'DEPARTMENT_HEAD_MANAGER'
        df.loc[df['participant_type'] == 'управленческий руководитель', 'auto_select_rule_type'] = 'FUNCTIONAL_HEAD_MANAGER'
        df.loc[df['participant_type'] == 'руководитель по иерархии', 'auto_select_rule_type'] = 'HIERARCHICAL_DEPARTMENT_HEAD_MANAGER'

        selectable_employee_list = ['руководитель отдела',
                                    'управленческий руководитель',
                                    'руководитель по иерархии',
                                    'произвольное количество']
        df.loc[df['participant_type'].isin(selectable_employee_list), 'participant_type'] = 'SELECTABLE_EMPLOYEE'

    @staticmethod
    def update_required(df):
        df.loc[df['required'] == 'обязательный', 'required'] = 'true'
        df.loc[df['required'] == 'необязательный', 'required'] = 'false'
        df.loc[df['participant_type'] == 'FIXED_EMPLOYEE', 'required'] = 'true'
        df.loc[df['required'].isna(), 'required'] = 'true'

    @staticmethod
    def update_participant_action_type(df):
        df.loc[df['participant_action_type'] == 'подписать', 'participant_action_type'] = 'SIGNING'
        df.loc[df['participant_action_type'] == 'согласовать', 'participant_action_type'] = 'APPROVING'
        df.loc[df['participant_action_type'] == 'обработать', 'participant_action_type'] = 'PROCESSING'

    @staticmethod
    def update_include_to_print_form_stamp(df):
        df.loc[df['include_to_print_form_stamp'] == 'да', 'include_to_print_form_stamp'] = 'true'
        df.loc[df['include_to_print_form_stamp'] == 'нет', 'include_to_print_form_stamp'] = 'false'
        df.loc[df['include_to_print_form_stamp'].isna(), 'include_to_print_form_stamp'] = 'true'

    @staticmethod
    def update_related_participant_id(df):
        rel_part_list = df.loc[df['related_participant_id'].notna(), 'related_participant_id']
        if rel_part_list.to_list():
            df.loc[df['related_participant_id'].notna(), 'related_participant_id'] = rel_part_list.str.split(' ', expand=True)[1]

        # Добавление related_participant_id, где необходимо
        r_index_list = (df.sort_values(by='created_date')
                        .reset_index(drop=True)
                        .index[df['related_participant_id'].notna()]
                        .to_list())
        related_list = list(map(int, df['related_participant_id'].loc[df['related_participant_id'].notna()].to_list()))
        related_list = [x - 1 for x in related_list]
        for i in range(len(r_index_list)):
            df.at[r_index_list[i], 'related_participant_id'] = df.at[related_list[i], 'id']

        head_manager_list = ['DEPARTMENT_HEAD_MANAGER',
                             'FUNCTIONAL_HEAD_MANAGER',
                             'HIERARCHICAL_DEPARTMENT_HEAD_MANAGER']
        df.loc[(df['auto_select_rule_type'].isin(head_manager_list) & df['related_participant_id'].isna()), 'related_participant_id'] = df.iloc[0, 0]
        df.loc[~df['auto_select_rule_type'].isin(head_manager_list), 'related_participant_id'] = 'null'


    @staticmethod
    def update_participant_signing_type(df):
        df.loc[df['participant_signing_type'] == 'укэп', 'participant_signing_type'] = 'QES'
        df.loc[df['participant_signing_type'] == 'унэп', 'participant_signing_type'] = 'CLOUD_NQES'
        df.loc[df['participant_signing_type'] == 'пэп госуслуги', 'participant_signing_type'] = 'PRR'
        df.loc[df['participant_signing_type'] == 'пэп hrlink', 'participant_signing_type'] = 'SES'
        df.loc[df['participant_signing_type'] == 'госключ', 'participant_signing_type'] = 'GOV_KEY'
        df.loc[df['participant_signing_type'] == 'любая', 'participant_signing_type'] = 'ANY_APPLICABLE'

    @staticmethod
    def update_unchangeable(df):
        df.loc[df['unchangeable'] == 'да', 'unchangeable'] = 'true'
        df.loc[df['unchangeable'] == 'нет', 'unchangeable'] = 'false'
        df.loc[df['unchangeable'].isna(), 'unchangeable'] = 'false'

    @staticmethod
    def autofill(df):
        with pd.option_context("future.no_silent_downcasting", True):
            df = (df.groupby("template_stage_id", as_index=False)
                  .apply(lambda s: s.bfill().ffill())
                  .reset_index(drop=True))
        return df

    @staticmethod
    def part_fill(file_uid, file_type):
        file = File(file_uid=file_uid, file_type=file_type, step='participant')
        # Создание пустого датафрейма participants
        df = file.formatted_data()
        part_df = file.get_empty_df()

        if file_type == 'APP':

            # Заполение датафрейма participants
            part_df.loc[len(part_df)] = ([uuid.uuid4()] +
                                         [0] +
                                         ['EMPLOYEE'] +
                                         ['SIGNING'] +
                                         ['ANY_APPLICABLE'] +
                                         ['Заявитель'] +
                                         ['null'] +
                                         [get_current_time()] +
                                         ['null'] +
                                         ['null'] +
                                         [np.NaN] +
                                         ['false'] +
                                         ['true'] +
                                         ['true'] +
                                         ['false'])

            # Добавление участников кроме получателей
            counter = 0
            stage_counter = 0
            for stage in df['stage_type'].fillna('null'):
                if stage.strip()[:6] == 'Этап №':
                    counter += 1
                    stage_counter += 1
                else:
                    try:
                        if df['link_to_fixed_employee'][counter] == 'финальный этап получатели' \
                                or df['participant_type'][counter + 1] == 'фиксированные получатели' \
                                or df['participant_type'][counter + 1] == 'выбираемые получатели':
                            return part_df

                        else:
                            part_df.loc[counter] = (
                                    [uuid.uuid4()] +
                                    [stage_counter] +
                                    [df['participant_type'][counter]] +
                                    [df['participant_action_type'][counter]] +
                                    ['SES'] +
                                    [df['stage_type'][counter]] +
                                    [df['link_to_fixed_employee'].fillna('null')[counter].rsplit('/', 1)[-1]] +
                                    [get_current_time()] +
                                    ['null'] +
                                    ['null'] +
                                    [df['related_participant_id'][counter]] +
                                    ['false'] +
                                    [df['include_to_print_form_stamp'][counter]] +
                                    [df['required'][counter]] +
                                    [df['unchangeable'][counter]]
                            )
                            counter += 1
                    except KeyError:
                        return part_df
            return part_df

        else:
            # Заполнение данных participants
            counter = 0
            stage_counter = -1
            for stage in df['stage_type'].fillna('null'):
                if str(stage)[:6] == 'Этап №':
                    counter += 1
                    stage_counter += 1
                elif str(stage)[:6] != 'Этап №':
                    part_df.loc[counter] = ([uuid.uuid4()] +
                                            [stage_counter] +
                                            [df['participant_type'][counter]] +
                                            [df['participant_action_type'][counter]] +
                                            [df['participant_signing_type'][counter]] +
                                            [df['stage_type'][counter]] +
                                            [df['link_to_fixed_employee'].fillna('null')[counter].rsplit('/', 1)[-1]] +
                                            [get_current_time()] +
                                            ['null'] +
                                            ['null'] +
                                            ['null'] +
                                            ['false'] +
                                            ['true'] +
                                            ['true'] +
                                            ['false'])
                    counter += 1
            return part_df

    @staticmethod
    def part_receiver_fill(receiver_type, part_df, stage_counter):
        counter = 0
        for i in range(len(receiver_type)):
            part_df.loc[len(part_df.index)] = ([uuid.uuid4()] +
                                               [stage_counter] +
                                               [receiver_type['receiver_type'].iloc[i]] +
                                               ['RECEIVING'] +
                                               ['null'] +
                                               [receiver_type['placeholder'].iloc[i]] +
                                               [receiver_type['link_to_fixed_employee'].iloc[i].rsplit('/', 1)[-1]] +
                                               [get_current_time()] +
                                               ['null'] +
                                               ['null'] +
                                               ['null'] +
                                               ['false'] +
                                               ['false'] +
                                               ['false'] +
                                               ['false'])
            counter += 1

        return part_df

    @staticmethod
    def part_fix_rows(part_df, file_type):
        # ДФ подписанты
        df_no_r = Participant(df=part_df, receiver=False).df
        # ДФ получатели
        df_r = Participant(df=part_df, receiver=True).df

        if file_type == 'APP':
            # Предварительные изменения данных на значения из нашей бд
            Participant.update_participant_type(df=df_no_r)
            Participant.update_participant_action_type(df=df_no_r)
            Participant.update_required(df=df_no_r)
            Participant.update_related_participant_id(df=df_no_r)
            Participant.update_include_to_print_form_stamp(df=df_no_r)
            Participant.update_unchangeable(df=df_no_r)

        else:
            Participant.update_participant_type(df=df_no_r)
            Participant.update_participant_action_type(df=df_no_r)
            Participant.update_participant_signing_type(df=df_no_r)

        # Заполнение пропусков из соседних полей (за исключением получателей)
        df_no_r = Participant.autofill(df_no_r)

        # Объединение получателей и подписантов
        df_all = df_no_r._append(df_r, ignore_index=True)
        df_all = df_all.fillna('null').sort_values('created_date').reset_index(drop=True)

        return df_all

    @staticmethod
    def get_receiver_type(file_uid, file_type):
        # Предварительная выгрузка, чтобы проверить сколько полей отсекать
        file = File(file_uid=file_uid, file_type=file_type)
        df = file.read_file(False)

        # Для документа
        if file_type == 'DOC':
            keeprows = df[df.iloc[:, 0] == 'Название участника'].index[0]
            df = df.head(keeprows)
            receiver_types = df.iloc[:, 1].fillna('Empty').to_list()
            links_to_fixed_employee = df.iloc[:, 2].fillna('null').to_list()

            # Проверка на наличие получателей
            if 'фиксированный' not in receiver_types and 'выбираемый' not in receiver_types:
                return None
            else:
                # Заполнение получателей
                del_list = []
                for i in range(len(receiver_types)):
                    if receiver_types[i] == 'фиксированный':
                        receiver_types[i] = 'FIXED_EMPLOYEE'
                    elif receiver_types[i] == 'выбираемый':
                        receiver_types[i] = 'SELECTABLE_EMPLOYEE'
                    else:
                        del_list.append(i)

                receiver_types = np.delete(receiver_types, del_list)
                links_to_fixed_employee = np.delete(links_to_fixed_employee, del_list)

                receiver_df = (pd.DataFrame(columns=['receiver_type',
                                                     'link_to_fixed_employee',
                                                     'placeholder'])
                               .assign(receiver_type=receiver_types,
                                       link_to_fixed_employee=links_to_fixed_employee,
                                       placeholder='Получатель'))

            return receiver_df.fillna('null')

        # Для заявления
        else:
            excel_file = File(file_uid=file_uid).path_excel
            # Проверка на наличие получателей через фиксированные получатели / выбираемые получатели
            try:
                skiprows = df[(df.iloc[:, 1] == 'выбираемые получатели') ^ (df.iloc[:, 1] == 'фиксированные получатели')].index[0]
                usecols = [0, 1, 2, 3]
                df = pd.DataFrame(pd.read_excel(excel_file, skiprows=skiprows, usecols=usecols))

            except IndexError:
                try:
                    # Проверка на наличие получателей через финальный этап получатели
                    skiprows = df[df.iloc[:, 3] == 'финальный этап получатели'].index[0]
                    usecols = [0, 1, 2, 3]
                    df = pd.DataFrame(
                        pd.read_excel(excel_file, skiprows=skiprows + 1, usecols=usecols))
                except IndexError:
                    return None
            # Создание и заполнение списков значений для подставновки в получателях
            receiver_placeholder = df.iloc[:, 0].fillna('Получатель').to_list()
            receiver_types = df.iloc[:, 1].fillna('Empty').to_list()
            links_to_fixed_employee = df.iloc[:, 3].fillna('null').to_list()
            receiver_types_new = []
            links_to_fixed_employee_new = []
            receiver_placeholder_new = []
            if links_to_fixed_employee == ['null'] and 'выбираемые получатели' not in receiver_types:
                return None

            if links_to_fixed_employee != ['null']:
                for i in range(len(links_to_fixed_employee)):
                    if links_to_fixed_employee[i] != 'null':
                        receiver_types_new.append('FIXED_EMPLOYEE')
                        links_to_fixed_employee_new.append(links_to_fixed_employee[i])
                        receiver_placeholder_new.append(receiver_placeholder[i])
            if 'выбираемые получатели' in receiver_types or 'выбираемый' in receiver_types:
                if 'выбираемые получатели' in receiver_types:
                    selectable_index = receiver_types.index('выбираемые получатели')
                else:
                    selectable_index = receiver_types.index('выбираемый')
                receiver_types_new.insert(selectable_index, 'SELECTABLE_EMPLOYEE')
                links_to_fixed_employee_new.insert(selectable_index, 'null')
                receiver_placeholder_new.insert(selectable_index, receiver_placeholder[selectable_index])

            receiver_df = (pd.DataFrame(columns=['receiver_type',
                                                 'link_to_fixed_employee',
                                                 'placeholder'])
                           .assign(receiver_type=receiver_types_new,
                                   link_to_fixed_employee=links_to_fixed_employee_new,
                                   placeholder=receiver_placeholder_new))

            return receiver_df.fillna('null')

    @staticmethod
    def part_missing_values(part_df, template_name, file_type):
        # Создаем массив ошибок, пропусков и др полезной инфы
        error_list = []

        # Проверка пропусков в participant_type
        err_p = len(part_df[part_df['participant_type'] == 'null'])
        if err_p > 0:
            error_list += [f'❗[{template_name}]\n Есть null в поле participant_type'
                           f'']
        # Проверка пропусков в participant_action_type
        err_a = len(part_df[part_df['participant_action_type'] == 'null'])
        if err_a > 0:
            error_list += [f'❗[{template_name}]\n Есть null в поле participant_action_type'
                           f'']
        # Проверка количества EMPLOYEE в маршруте
        err_e = len(part_df[part_df['participant_type'] == 'EMPLOYEE'])
        if err_e > 1:
            error_list += [f'❗[{template_name}]\n В маршруте более одного EMPLOYEE']
        # Проверка количества EMPLOYER в маршруте
        err_er = len(part_df[part_df['participant_type'] == 'EMPLOYER'])
        if err_er > 1:
            error_list += [f'❗[{template_name}]\n В маршруте более одного EMPLOYER']

        # Добавляем дф без получателей
        df_no_r = part_df[part_df['participant_action_type'] != 'RECEIVING']
        # Проверка пропусков в participant_signing_type
        err_s = len(df_no_r[df_no_r['participant_signing_type'] == 'null'])
        if err_s > 0:
            error_list += [f'❗[{template_name}]\n Есть null в поле participant_signing_type'
                           f'']
        # Проверка пропусков в employee_id у FIXED_EMPLOYEE
        err_f = len(part_df[(part_df['participant_type'] == 'FIXED_EMPLOYEE') & (df_no_r['employee_id'] == 'null')])
        if err_f > 0:
            error_list += [f'❗[{template_name}]\n Есть null в поле employee_id у FIXED_EMPLOYEE']
            #f'❗[{template_name}]\n Есть null в поле' + '```\nemployee_id\n```' + 'у FIXED_EMPLOYEE']

        # Проверка корректности заполнения uuid в employee_id
        fixed_id_list = (part_df[part_df['employee_id'] != 'null']['employee_id'].
                         dropna(inplace=False).to_list())
        if len(fixed_id_list) > 0:
            for fixed_id in fixed_id_list:
                try:
                    uuid_obj = uuid.UUID(fixed_id, version=4)
                except ValueError:
                    error_list += [f'❗[{template_name}]\n В поле employee_id'
                                   f' невереное значение: {fixed_id}']

        # Проверка обязательный + необязательный на одном этапе
        if part_df['template_stage_id'].nunique() != part_df.groupby(part_df['required'])['template_stage_id'].nunique().sum():
            error_list += [f'❗[{template_name}]\n В одном из этапов есть обязательный и необязательный участник']

        # Проверка ответственный не один на этапе
        if 'RESPONSIBLE' in part_df['participant_type'].to_list():
            resp_stage_id = part_df.loc[part_df['participant_type'] == 'RESPONSIBLE', 'template_stage_id'].to_list()[0]
            resp_stage_id_list = part_df.loc[part_df['template_stage_id'] == resp_stage_id, 'id'].astype(str).to_list()
            if len(resp_stage_id_list) > 1:
                error_list += [f'❗[{template_name}]\n На этапе с RESPONSIBLE указано больше одного участника']

        if file_type == 'DOC':

            # Проверка EMPLOYEE + EMPLOYER на одном этапе
            if err_e == 1 and err_er == 1:
                emp_id = part_df[part_df['participant_type'] == 'EMPLOYEE']['template_stage_id'].to_string(index=False)
                empr_id = part_df[part_df['participant_type'] == 'EMPLOYER']['template_stage_id'].to_string(index=False)
                if emp_id == empr_id:
                    error_list += [f'❗[{template_name}]\n EMPLOYER и EMPLOYEE в одном этапе']
                # Проверка видов подписей у сотрудника
                emp_sgn = part_df[part_df['participant_type'] == 'EMPLOYEE']['participant_signing_type'].to_string(
                    index=False)
                if emp_sgn == 'SES':
                    error_list += [f'❗[{template_name}]\n В поле signing_type'
                                   f' невереное значение у EMPLOYEE - SES']
                # Проверка видов подписей у рук-ля
                empr_sgn = part_df[part_df['participant_type'] == 'EMPLOYER']['participant_signing_type'].to_string(index=False)
                if empr_sgn != 'QES':
                    error_list += [
                        f'❗[{template_name}]\n В поле signing_type'
                        f' невереное значение у EMPLOYER - {empr_sgn}']
            # Проверка видов подписей у участников:
            part_sgn = list(
                df_no_r.loc[part_df['participant_type'] == 'FIXED_EMPLOYEE', 'participant_signing_type'].to_string(index=False)
            )
            if 'PRR' in part_sgn:
                error_list += [f'❗[{template_name}]\n В поле signing_type'
                               f' невереное значение у FIXED_EMPLOYEE - PRR']
            part_sgn = list(
                df_no_r.loc[part_df['participant_type'] == 'SELECTABLE_EMPLOYEE', 'participant_signing_type'].to_string(index=False)
            )
            if 'PRR' in part_sgn:
                error_list += [
                    f'❗[{template_name}]\n В поле signing_type'
                    f' невереное значение у SELECTABLE_EMPLOYEE - PRR']


        else:
            len_rel_part = len(part_df['related_participant_id'][
                                   (part_df['auto_select_rule_type'] == 'DEPARTMENT_HEAD_MANAGER') ^
                                   (part_df['auto_select_rule_type'] == 'FUNCTIONAL_HEAD_MANAGER')])

            len_hier_part = len(part_df['related_participant_id'][
                                    part_df['auto_select_rule_type'] == 'HIERARCHICAL_DEPARTMENT_HEAD_MANAGER'])

            len_rel_emp = len(part_df[part_df['related_participant_id'] == part_df.iloc[0, 0]])

            if len_rel_part > 0 and len_hier_part > 0:
                error_list += [f'❗[{template_name}]\n В поле auto_select_rule_type'
                               f' указаны несочитаемые значения.']
            elif (len_hier_part > 0 or len_rel_part > 0) and len_rel_emp == 0:
                error_list += [f'❗[{template_name}]\n В поле related_participant_id'
                               f' отсутствует заявитель (этап №1).']
        return error_list


class LegalEntity:
    @staticmethod
    def legal_entity_check(file_uid, file_type, part_df):
        file = File(file_uid=file_uid, file_type=file_type)
        df = file.read_file(False)
        try:
            temp_df = df[df.iloc[:, 0] == 'Привязка к юрлицу'].fillna('null')
            legal_entity = temp_df.iloc[:, 1].to_string(index=False)
            if legal_entity == 'null':
                return_value = None
            else:
                return_value = 'legal_entity'
        except IndexError:
            return_value = None

        if len(part_df[part_df['employee_id'] != 'null']) > 0:
            return_value = 'fixed_employee'

        return return_value
