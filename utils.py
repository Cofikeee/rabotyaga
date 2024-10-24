import csv
import shutil
import pandas as pd
from pathlib import Path
from aiogram.types import FSInputFile
from tabulate import tabulate


def get_current_time():
    return pd.Timestamp.now(tz='UTC')


def get_route_type(file_uid):
    excel_path = f'_worker/data/in/{file_uid}.xlsx'
    df = pd.DataFrame(pd.read_excel(excel_path))
    if 'Привязка к юрлицу' in df.iloc[:, 0].to_list():
        return 'DOC'
    else:
        return 'APP'


def pretty_printer(log_text, new_id, stage, actual_file_uid, edit_route_id):
    df_name = pd.DataFrame(pd.read_excel(f'_worker/data/in/{actual_file_uid}.xlsx'))
    try:
        template_name = df_name.iloc[0, 1].strip()
    except AttributeError:
        template_name = df_name.iloc[0, 1]
    with open(f'_worker/data/out/preview_{new_id}.txt', 'a') as pretty_output:
        if stage == 'template' and edit_route_id is None:
            pretty_output.write('\n' + '/'*240 + '\n')
            pretty_output.write(f'[{stage.upper()}]' + 29*'=' + f'[{template_name}]')
            pretty_output.write(f'\n{log_text}\n\n')
        elif stage == 'stage':
            pretty_output.write(f'[{stage.upper()}]' + 31*'=' + f'[{template_name}]')
            pretty_output.write(f'\n{log_text}\n\n')
        elif stage == 'participant':
            pretty_output.write(f'[{stage.upper()}]' + 25*'=' + f'[{template_name}]')
            pretty_output.write(f'\n{log_text}\n\n')
        elif stage == 'legal_entity':
            pretty_output.write(f'[{stage.upper()}]' + 25*'=' + f'[{template_name}]')
            pretty_output.write(f'\n{log_text}\n\n')

    return


def get_file_name(file_namexl):
    file_name = file_namexl.split('.')[0]
    return file_name


def create_output_dir(file_uid):
    # Создание директории для csv файлов
    Path(f'_worker/data/out/{file_uid}').mkdir(parents=True, exist_ok=True)
    return


def add_id(file_uid_list):
    # Функция добавляющая записи связок group_id-<uid
    df_id = pd.read_csv(f'files/id_uid_list.csv')
    new_id = int(df_id.iloc[-1, 0]) + 1
    for file_uid in file_uid_list:
        df_id.loc[len(df_id)] = new_id, file_uid
        df_id.to_csv('files/id_uid_list.csv', index=False)
    return


def query_printer(step, path, file_group_id, edit_route_id):

    def convert(value):
        # Конвертация всех значений в string
        for type in [int, float]:
            try:
                return type(value)
            except ValueError:
                continue
        return value

    string_sql = ''
    if step == 'template':
        table = 'signing_route_template'
    elif step == 'stage':
        table = 'signing_route_template_stage'
    elif step == 'participant':
        table = 'signing_route_template_participant'
    else:
        table = 'signing_route_template_legal_entity'


    if step == 'template' and edit_route_id is not None:
        with (open(f"files/delete_old_route.sql", "r") as read,
              open(f"_worker/data/out/string_SQL_{file_group_id}.txt", "a") as write):
            for line in read:
                write.write(line.replace("$templateId", f"{edit_route_id}"))
    else:
        with open(path, 'r') as file:
            reader = csv.reader(file)
            headers = ','.join(next(reader))
            string_sql += f';\nINSERT INTO ekd_ekd.{table}({headers}) VALUES'
            coma = ''
            for row in reader:
                row = [convert(x) for x in row].__str__()[1:-1]
                string_sql += f'{coma}\n({row})'
                coma = ','

    with open(f"_worker/data/out/string_SQL_{file_group_id}.txt", "a") as text_file:
        text_file.write(f'{string_sql}')
    return


def add_string(string, file_group_id):
    # Функция добавляющая строку к sql-скрипту
    with open(f"_worker/data/out/string_SQL_{file_group_id}.txt", "a") as text_file:
        text_file.write(f'{string}')
    return


def unload_sql_tg(file_uid_list, edit_route_id):

    file_group_id = file_uid_list[0]
    stages = ['template', 'stage', 'participant', 'legal_entity']
    df_id = pd.read_csv(f'files/id_uid_list.csv')
    new_id = int(df_id.iloc[-1, 0])
    add_string('BEGIN', file_group_id)

    for file_uid in file_uid_list:
        # Определение типа файла
        file_type = get_route_type(file_uid)
        df_name = pd.DataFrame(pd.read_excel(f'_worker/data/in/{file_uid}.xlsx'))
        add_string(f';\n-- [{df_name.iloc[0, 1]}]', file_group_id)
        for stage in stages:
            try:
                path_stage = f'_worker/data/out/{file_uid}/_{file_type}_{stage}_{file_uid}.csv'
                df = pd.read_csv(path_stage, keep_default_na=False, sep=',')
                pretty_printer(tabulate(df, headers='keys', tablefmt='psql', showindex=False),
                               new_id,
                               stage,
                               file_uid,
                               edit_route_id)
                query_printer(stage, path_stage, file_group_id, edit_route_id)
                if stage == 'legal_entity':
                    part_df = pd.read_csv(f'_worker/data/out/{file_uid}/_{file_type}_participant_{file_uid}.csv')
                    fixed_id_list = part_df.dropna(subset='employee_id')['employee_id'].to_list()
                    if len(fixed_id_list) > 1:
                        for i in range(len(fixed_id_list)):
                            fixed_id_list[i] = f'\'{fixed_id_list[i]}\''
                        fixed_id_list = ','.join(fixed_id_list)
                        with (open(f"files/legal_entity_check.sql", "r") as read,
                              open(f"_worker/data/out/string_SQL_{file_group_id}.txt", "a") as write):
                            for line in read:
                                write.write(line.replace("$ids_list", f"{fixed_id_list}")
                                            .replace("$template_name", f"{df_name.iloc[0, 1]}")
                                            .replace("-- ", f"-- [{df_name.iloc[0, 1]}] "))
            except FileNotFoundError:
                pass

    add_string(';\nCOMMIT;\n', file_group_id)

    if edit_route_id is None:
        script_name = f'_worker/data/out/add_route_{new_id}.sql'
    else:
        script_name = f'_worker/data/out/edit_route_{new_id}.sql'

    with (open(f"_worker/data/out/string_SQL_{file_group_id}.txt", "r") as read,
          open(script_name, 'w') as write):

        for line in read:
            write.write(line
                        .replace("'null'", "null")
                        .replace("'(SELECT id from ekd_ekd.client)'", "(SELECT id from ekd_ekd.client)")
                        .replace('"', ''))

    sql_query = script_name

    Path(f"_worker/data/out/string_SQL_{file_group_id}.txt").unlink()
    for file_uid in file_uid_list:
        shutil.rmtree(f'_worker/data/out/{file_uid}/')

    pretty_output = f'_worker/data/out/preview_{new_id}.txt'
    print(open(pretty_output, 'r').read())
    return FSInputFile(sql_query), FSInputFile(pretty_output)

