import uuid
import pandas as pd
from _worker.classes import File, Alert, Template, Stage, Participant, LegalEntity



def signing_route_template_csv(file_uid, file_type):
    file = File(file_uid=file_uid, file_type=file_type, step='template')
    template = Template(file_uid=file_uid, file_type=file_type)
    # Получение названия типа маршрута
    file_type_name = template.update_file_type()

    # Получение названия маршрута
    template_name = template.update_template_name()

    # Создание пустого датафрейма template
    template_df = file.get_empty_df()

    # Заполение датафрейма template
    template_df.loc[len(template_df)] = ([uuid.uuid4(),
                                          '(SELECT id from ekd_ekd.client)',
                                          template_name,
                                          file_type_name])

    file.df = template_df
    # Выгрузка датафрейма в csv
    file.csv_printer()

    # Проверка на наличие названия у маршрута:
    return Alert.check_template_name(template_name)


def signing_route_template_stage_csv(file_uid, file_type, edit_route_id):
    file = File(file_uid=file_uid, file_type=file_type, step='stage')
    template = Template(file_uid=file_uid, file_type=file_type)
    # Получаем template_id
    if edit_route_id is None:
        template_id = template.get_template_id()
    else:
        template_id = edit_route_id

    # Создание и заполнение датафрейма
    stages_df = Stage.stages_fill(file_uid, file_type, template_id)
    # Удаление дубликатов
    stages_df = Stage.stages_drop_duplicates(stages_df)

    # Фиксим данные
    Stage.update_stage_completeness_condition(stages_df)
    Stage.update_can_delete_before_stage_completed(stages_df)

    receiver_type = Participant.get_receiver_type(file_uid, file_type)

    if receiver_type is not None and len(receiver_type) > 0:
        stages_df = Stage.stages_receiver_fill(stages_df, template_id, file_type)

    for i in range(len(stages_df)):
        stages_df.loc[i, 'index_number'] = i

    # Фиксим возможность отозвать заявление
    if file_type == 'APP':
        stages_df = Stage.stages_fix_can_delete(stages_df)

    file.df = stages_df
    # Выгрузка датафрейма в csv, выгрузка в лог
    file.csv_printer()

    return


def signing_route_template_participant_csv(file_uid, file_type):

    # Создание и заполнение датафрейма, выгрузка в лог
    part_df = Participant.part_fill(file_uid, file_type).reset_index(drop=True)

    receiver_type = Participant.get_receiver_type(file_uid, file_type)

    subset = ['template_stage_index',
              'participant_type',
              'participant_action_type',
              'participant_signing_type',
              'placeholder',
              'employee_id',
              'include_to_print_form_stamp',
              'required']

    with pd.option_context("future.no_silent_downcasting", True):
        part_df = (part_df.groupby("template_stage_index", as_index=False)
                   .apply(lambda s: s.bfill().ffill())
                   .drop_duplicates(subset=subset)
                   .reset_index(drop=True))

    if receiver_type is not None:
        stage_counter = part_df['template_stage_index'].iloc[len(part_df) - 1] + 1
        part_df = Participant.part_receiver_fill(receiver_type, part_df, stage_counter)


    stages_df = pd.read_csv(File(file_uid=file_uid, file_type=file_type, step='stage').path_df_to_csv)
    stages_df = stages_df[['id', 'index_number']].rename(columns={'id': 'template_stage_id',
                                                                  'index_number': 'template_stage_index'})

    part_df = pd.merge(part_df, stages_df, on='template_stage_index', how='left')
    part_df_rows = list(part_df)
    part_df_rows[1], part_df_rows[-1] = part_df_rows[-1], part_df_rows[1]
    part_df = part_df.loc[:, part_df_rows]
    part_df.drop(part_df.columns[-1], axis=1, inplace=True)

    part_df = Participant.part_fix_rows(part_df, file_type)

    # Выгрузка датафрейма в csv, выгрузка в лог
    file = File(file_uid=file_uid, file_type=file_type, step='participant', df=part_df)
    file.csv_printer()

    df_name = pd.DataFrame(pd.read_excel(File(file_uid=file_uid).path_excel))
    try:
        template_name = df_name.iloc[0, 1].strip()
    except AttributeError:
        template_name = df_name.iloc[0, 1]
    if file_type == 'DOC':
        if LegalEntity.legal_entity_check(file_uid, file_type, part_df) == 'fixed_employee':
            return Participant.part_missing_values(part_df, template_name, file_type), True
        elif LegalEntity.legal_entity_check(file_uid, file_type, part_df) == 'legal_entity':
            return (Participant.part_missing_values(part_df, template_name, file_type) +
                    [f'❗️[{template_name}] Есть привязка к ЮЛ, но нет fixed_employee'], False)
    return Participant.part_missing_values(part_df, template_name, file_type), False


def signing_route_template_legal_entity_csv(file_uid):
    file_type = 'DOC'
    file = File(file_uid=file_uid, file_type=file_type, step='legal_entity')
    template = Template(file_uid=file_uid, file_type=file_type)
    #participant = File(file_uid=file_uid, file_type=file_type, step='participant')
    # Создание пустого датафрейма из шаблона датафрейма
    le_df = file.get_empty_df()

    # Получение id маршрута документа
    template_id = template.get_template_id()

    # Получение id фиксированного сотрудника
    part_df = pd.read_csv(File(file_uid=file_uid, file_type=file_type, step='participant').path_df_to_csv)
    try:
        fixed_employee_id = part_df.dropna(subset='employee_id')['employee_id'].to_list()[0]
        le_df.loc[len(le_df)] = ([uuid.uuid4(),
                                  template_id,
                                  f'(SELECT legal_entity_id FROM ekd_ekd.employee WHERE id IN (\'{fixed_employee_id}\'))'])
        # Выгрузка датафрейма в csv, выгрузка в лог
        file.df = le_df
        file.csv_printer()

    except IndexError:
        pass





