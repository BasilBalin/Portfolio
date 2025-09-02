# imports

import os
import socket
import sys

import pandas as pd

from sqlalchemy import create_engine, text

# предполагается, что модуль user_functions находится
# в одном  каталоге с модулем get_dicts_v_06
from user_functions import choose_dicts, get_path, print_table, set_val

path_ = get_path()
sys.path.append(path_)

from paths_and_constants import main as mn, schema


# Константы и их производные

user, pwd = mn()


# user functions

# Отправка запроса
def main(str_numbers_of_tables=None):

    dict_keyboard_input_2_dict, str_max_num_key, engine = print_table()

    table, user_choices = choose_dicts(
        dict_keyboard_input_2_dict,
        str_max_num_key,
        str_numbers_of_tables,
    )

    if not isinstance(table, list):
        (tables := []).append(tuple(table))
        # print(f'tables_0 = {tables}')

    else:
        tables = table
        # print(f'tables_1 = {tables}')


    dict_dicts = {}

    if user_choices == '0':
        user_choices = [str(_) for _ in range(1, int(str_max_num_key) + 1)]

    # Выполняем запрос с использованием контекстного менеджера
    ls_fns, ls_last_update_timedates = set_val(
        schema,
        'df_tables_info',
        user_choices,
        'how_many_times_was_downloaded',
        'table_id',
        engine,
    )

    # и сохраняем результат выполнения в DataFrame.

    for i, table in enumerate(tables):

        try:
            # Sqlalchemy автоматически установит названия колонок
            # такими же, как у таблицы в БД
            response = pd.read_sql_table(table[0], engine, schema=schema)

            # print('\n---------------------------------------------')
            # print(table)
            # print(response.head(2)
            # )

            dict_dicts[table[0]] = (
                response, ls_fns[i], ls_last_update_timedates[i],
            )

        except Exception as e:
            print(f'\nНе удалось загрузить словарь "{table[0]}" из-за {e}')

    if len(tables) == 1:
        print('\nСловарь записан в словарь словарей `dict_dicts`')

    else:
        print('\nСловари записаны в словарь словарей `dict_dicts`')

    return dict_dicts, tables


if __name__ == "__main__":

    print('\nЯ вызван как основная программа!')
    dict_dicts, tables = main('0')

else:

    print('\nНачинаю загружать доп. переменные и словари')
