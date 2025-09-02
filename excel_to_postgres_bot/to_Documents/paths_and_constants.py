#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import socket

# получаем имя компьютера
hostname = socket.gethostname()


if hostname == '304-007':
    path_ = r'C:\Users\Public\dictionaries'
else:
    path_ = os.path.expanduser('~\\Documents\\')

# Указываем директорию с документами пользователя
ROOT_DOCUMENTS = os.path.expanduser('~\\Documents\\')

# относительный путь к файлу с данными пользователя для подключения к БД
cinfo = rf'{ROOT_DOCUMENTS}sql_user_info.txt'
cinfo_0_tam = rf'{ROOT_DOCUMENTS}sql_user_info_0_tam.txt'
cinfo_1_mhcd_main = rf'{ROOT_DOCUMENTS}sql_user_info_1_mhcd_main.txt'

# IPv4-address of PC
str_server_ip = '11.222.111.222'

# название схемы данных
schema = 'dictionaries'

tup_server_1_big = (str_server_ip, 5432, 'mhcd_eas')


# читаем cinfo из текстового файла
def get_sql_user_info_from_txt_file(cinfo):
    with open(cinfo, 'r', encoding='utf-8') as f:
        [user, pwd] = [line.strip('\n') for line in f.readlines()]
    return [user, pwd]


def main():
    print(f'\nhostname = "{hostname}"')

    if os.path.exists(cinfo):
        [user, pwd] = get_sql_user_info_from_txt_file(cinfo)
        print(f'\nФайл с авторизационными данными пользователя (роль) '
              f'"sql_user_info.txt" найден по адресу "{cinfo}"')
    else:
        raise AssertionError('\nФайл с авторизационными данными пользователя '
                             '(роль) "sql_user_info.txt" НЕ НАЙДЕН по адресу '
                             f'"{cinfo}"')

    return user, pwd


if __name__ == "__main__":

    print('\nЯ вызван как основная программа!')

else:

    print('\nНачинаю загружать пути')
    main()
