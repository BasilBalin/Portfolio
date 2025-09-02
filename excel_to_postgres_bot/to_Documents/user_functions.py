#!/usr/bin/python
# -*- coding: utf-8 -*-

from sqlalchemy import create_engine, text

import os
import socket
import sys
from tqdm import tqdm

import pandas as pd

def get_path():
    # получаем имя компьютера
    hostname = socket.gethostname()

    if hostname == '304-007':
        path_ = r'C:\Users\Public\dictionaries'
    else:
        path_ = os.path.expanduser('~\\Documents\\')

    return path_


path_ = get_path()
sys.path.append(path_)

from dict_sql_queries import dict_sql_queries
from paths_and_constants import cinfo, schema, tup_server_0_tam, tup_server_1_big


# читаем cinfo из текстового файла
def get_sql_user_info_from_txt_file(cinfo):
    with open(cinfo, 'r', encoding='utf-8') as f:
        [user, pwd] = [line.strip('\n') for line in f.readlines()]
    return [user, pwd]


def create_sql_connection(user, pwd, server_info):
    # Задаём параметры подключения к БД,
    # их можно узнать у администратора БД.
    db_config = {
        'user': user,  # имя пользователя
        'pwd': pwd,  # пароль
        'host': server_info[0],  # адрес сервера
        'port': server_info[1],  # порт подключения
        'db': server_info[2],  # название базы данных
    }

    # Формируем строку соединения с БД.
    connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(
        db_config['user'],
        db_config['pwd'],
        db_config['host'],
        db_config['port'],
        db_config['db'],
    )

    # Подключаемся к БД.
    return create_engine(connection_string)


def set_val(
        schema,
        table_name,
        ls_dict_numbers,
        column_name,
        pkey_name,
        engine,
):

    get_tables_info_4_clmns = (
        dict_sql_queries['get_tables_info_4_clmns_raw']
        .format(
            pkey_name=pkey_name,
            column_name=column_name,
            schema=schema,
            table_name=table_name,
            sql_formatted_list=','.join([f"'{_}'" for _ in ls_dict_numbers]),
        )
    )

    # применяем изменения в цикле к таблице SQL
    with engine.connect() as conn:

        # читаем таблицу с инфо о словарях
        df = pd.read_sql_query(get_tables_info_4_clmns, engine).sort_values(
            by=['table_id'],
        )

        # прибавляем 1 к счётчикам загрузки таблиц
        ls_column_vals = df[column_name].apply(lambda x: x + 1).tolist()
        ls_last_update_timedates = df['last_update_timedate'].tolist()

        ls_fns = df['last_sender'].tolist()

        # print(ls_last_update_timedates)

        for pkey_val, column_val in list(zip(ls_dict_numbers, ls_column_vals)):

            set_val = (
                dict_sql_queries['set_val_raw']
                .format(
                    schema=schema,
                    table_name=table_name,
                    column_name=column_name,
                    column_val=column_val,
                    pkey_name=pkey_name,
                    pkey_val=pkey_val,
                    )
            )

            conn.execute(text(set_val))
            conn.commit()  # Если используете явные транзакции

    return ls_fns, ls_last_update_timedates


# create sql connection
def print_table():
    [user, pwd] = get_sql_user_info_from_txt_file(cinfo)

    engine = create_sql_connection(
        user=user, pwd=pwd, server_info=tup_server_1_big,
    )

    with engine.connect() as connection:

        df_tables_info = pd.read_sql_table(
            table_name='df_tables_info',
            con=connection,
            schema=schema,
        )

    # словарь клавиш, псевдонимов и изменяемых
    # частей имён шаблонов файлов со словарями
    dict_keyboard_input_2_dict = {}

    for line_number in sorted(df_tables_info['table_id']):

        dict_keyboard_input_2_dict[line_number] = (
            df_tables_info.loc[df_tables_info[
                'table_id'
                ] == line_number, 'df_name'].values[0],
            df_tables_info.loc[df_tables_info[
                'table_id'
                ] == line_number, 'main_part_of_file_name'].values[0],
        )

    # максимальные длины строк для формирования таблицы markdown
    int_max_len_of_dict_aliases = max(
        [len(_[0]) for _ in dict_keyboard_input_2_dict.values()]
    )
    int_max_len_of_dict_names = max(
        [len(_[1]) for _ in dict_keyboard_input_2_dict.values()]
    )
    int_len_3d_column = len('Клавиша')
    # наибольшее число, соответствующее цифровой клавише / цифровым
    # клавишам для выбора словаря
    str_max_num_key = max(dict_keyboard_input_2_dict.keys())

    # общие начало и конец имен файлов словарей
    b = 'Словарь_'
    e = '_20YY_MM_DD.xlsx'
    # общая максимальная длина шаблона имени словаря
    int_max_len_of_dict_names += len(b) + len(e)

    # строка формата `f`, формирующая таблицу markdown
    f = f"| %{-int_max_len_of_dict_aliases}s | %{-int_max_len_of_dict_names}s | %{int_len_3d_column}s |"

    # верх таблицы markdown
    txt = f'''
    ```
{f % (f"table", f"Маска имени файла", f"Клавиша")}
| {(int_max_len_of_dict_aliases) * '-'} | {(int_max_len_of_dict_names) * '-'} | {(int_len_3d_column) * '-'} |
{f % (f"all", f"<Все словари>", (f"0").center(int_len_3d_column))}'''

    # низ таблицы markdown
    for key, (alias, name) in (dict_keyboard_input_2_dict.items()):
        txt = '\n'.join([txt, (f % (f"{alias}", f"{b}{name}{e}", f"{str(key).center(int_len_3d_column)}"))])

    # добавим окончание к таблице markdown
    txt = '\n'.join([txt, '```'])

    print(txt)
    # print(int_max_len_of_dict_aliases)

    # соберём все кортежи для ненулевых чисел в список для клавиши `0`
    dict_keyboard_input_2_dict[0] = []
    for i in range(1, int(str_max_num_key) + 1):
        dict_keyboard_input_2_dict[0].append(dict_keyboard_input_2_dict[i])

    return dict_keyboard_input_2_dict, str_max_num_key, engine


# Выбор словаря(словарей) - исх. код
def choose_dicts(
    dict_keyboard_input_2_dict,
    str_max_num_key,
    str_numbers_of_tables,
):
    if str_numbers_of_tables is None:
        user_s_answer = input(
            'Введите номер словаря из таблицы сверху или'
            ' несколько номеров, разделённых пробелом: ',
        )
    else:
        user_s_answer = str_numbers_of_tables

    flag = True
    while flag:
        if ' ' in user_s_answer:
            user_s_answer = user_s_answer.split()
        else:
            user_s_answer = [user_s_answer]

        for string in user_s_answer:
            try:
                # Попытка преобразовать ввод в число
                # и проверить его в диапазоне
                if int(string) in range(0, int(str_max_num_key) + 1):
                    flag = False
                    break
                else:
                    user_s_answer = input(
                        f'{string} - '
                        'Неверный номер, попробуйте снова: ',
                    )
            except ValueError:
                # Если произошла ошибка преобразования, запросить ввод снова
                user_s_answer = input(
                    f'Введите число от 0 до {str_max_num_key}: ',
                )

    user_s_answer = sorted(user_s_answer)

    # print(dict_keyboard_input_2_dict)

    if len(user_s_answer) > 1 and '0' not in user_s_answer:

        table = []
        for table_number in user_s_answer:

            table.append(dict_keyboard_input_2_dict[int(table_number)])

        # print(f'table = {table}')

        to_print = [tup[0] for tup in table]
        print(f'\nВы выбрали: {", ".join(to_print)}')

    else:

        user_s_answer = user_s_answer[0]

        table = dict_keyboard_input_2_dict[int(user_s_answer)]
        to_print = [tup[0] for tup in dict_keyboard_input_2_dict[
            int(user_s_answer)
            ]] if user_s_answer == '0' else dict_keyboard_input_2_dict[
                int(user_s_answer)
                ][0]
        print(f'\nВы выбрали: {to_print}')

    user_choices = user_s_answer

    return table, user_choices


def rows_to_sql(
    df: pd.core.frame.DataFrame,
    rows: int,
    table_schema: str,
    table_name: str,
    server_info: tuple,
):
    """функция "забрасывает" очередную "порцию" данных датафрейма `df`
       в виде `rows` строк в таблицу `table_name` SQL-БД

    Args:
        df (pd.core.frame.DataFrame): весь загружаемый датафрейм
        rows (int): количество строк, после которого происходит коммит
        table_schema (str): имя схемы БД SQL
        table_name (str): имя таблицы в схеме БД SQL
        server_info (tuple): данные сервера для подключения
    """
    for i in tqdm(range(0, len(df), rows)):
        print(len(df), rows)
        [user, pwd] = get_sql_user_info_from_txt_file(cinfo)
        engine = create_sql_connection(user, pwd, server_info)
        with engine.connect() as conn:
            df.loc[i:i + rows - 1, :].to_sql(
                table_name,
                conn,
                schema=table_schema,
                if_exists='append',
                index=False,
            )

# +++++++++++ Функция подтверждения продолжения выполнения скрипта +++++++++++


def confirm_and_continue():
    response = input(
        "Структура таблиц отличается, останавливаем? (да/нет): ",
        ).strip().lower()

    if response == 'нет':
        print("Сontinue...")
        # Здесь размещаем код для продолжения выполнения скрипта
    elif response == 'да':
        print("Выполнение скрипта прервано.")
        sys.exit()  # Завершает выполнение всего скрипта
    else:
        print("Некорректный ввод. Пожалуйста, введите 'да' или 'нет'.")
        confirm_and_continue()  # Рекурсия для повторного запроса


def df_2_sql(df, table_schema, table_name, ls_pk, ls_index, rows=500_000):

    [user, pwd] = get_sql_user_info_from_txt_file(cinfo)
    engine = create_sql_connection(user, pwd, tup_server_1_big)
    with engine.connect() as conn:

        df_to_compare_columns = pd.read_sql_query(dict_sql_queries[
            'get_tables_info_2_clmns_raw'
            ].format(
            table_schema=table_schema,
            table_name=table_name,
        ), conn)

    if len(df_to_compare_columns) == 0:
        df.loc[:9, :].to_sql(
            table_name,
            engine,
            schema=table_schema,
            if_exists='append',
            index=False,
        )

        with engine.connect() as conn:
            query = dict_sql_queries['add_primary_key_raw'].format(
                table_schema=table_schema,
                table_name=table_name,
                csv=f"{', '.join(ls_pk)}",
                )
            # создание первичного ключа после сохранения данных
            if ls_pk:
                # print(query)
                conn.execute(text(query))
                conn.commit()  # Если используете явные транзакции

            # Создание индекса после сохранения данных
            if ls_index:
                for column_name in ls_index:
                    add_index = dict_sql_queries['create_index_raw']
                    query = add_index.format(
                        column_name=column_name,
                        table_schema=table_schema,
                        table_name=table_name,
                    )
                    # print(query)
                    conn.execute(text(query))
                    conn.commit()  # Если используете явные транзакции

        rows_to_sql(
            df.loc[10:, :], rows, table_schema, table_name, tup_server_1_big,
        )

    else:
        if sorted(df.columns) != sorted(df_to_compare_columns['column_name']):
            confirm_and_continue()
        rows_to_sql(df, rows, table_schema, table_name, tup_server_1_big)
