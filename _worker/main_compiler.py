from _worker.classes import File
from _worker.csv_compilers import (signing_route_template_csv,
                                   signing_route_template_participant_csv,
                                   signing_route_template_legal_entity_csv,
                                   signing_route_template_stage_csv)


def main(file_uid, edit_route_id):
    new_file = File(file_uid)

    # Создание директории для csv файлов
    new_file.create_output_dir()

    # Определение типа файла
    file_type = new_file.get_route_type()

    # Создание csv файлов для таблиц template > stages > participants > *legal_entity
    missing_values_t = signing_route_template_csv(file_uid, file_type)
    signing_route_template_stage_csv(file_uid, file_type, edit_route_id)
    missing_values_p, need_le = signing_route_template_participant_csv(file_uid, file_type)

    # Создание csv файла для таблицы legal_entity при необходимости
    if file_type == 'DOC' and need_le is True:
        signing_route_template_legal_entity_csv(file_uid, edit_route_id)

    # Собираем ошибки / инфу про привязку к ЮЛ
    missing_values = missing_values_t + missing_values_p

    return missing_values


if __name__ == '__main__':
    main()
