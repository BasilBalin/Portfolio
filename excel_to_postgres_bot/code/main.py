# -*- coding: utf-8 -*-
# !/usr/bin/python

# befor running:
#   * create command prompt terminal
#   * input:
#     cd C:\Users\admin\python\projects\tg_bot_for_sql\code & python C:\Users\admin\python\projects\tg_bot_for_sql\code\bot_mhcd_06_with_with.py

import asyncio
import logging
import os
import re
from datetime import datetime as dt_, timedelta
from pathlib import PurePath
import time

from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.types import InputFile

import matplotlib.pyplot as plt

from my_constants_2 import db_configs, sql_requests, texts
from my_constants_2 import dict_dows, tg_chat_ids
from my_constants_2 import dict_file_names, dict_regexes

import numpy as np

import pandas as pd

from sqlalchemy import create_engine, text

import socket


# Устанавливаем уровень логирования
logging.basicConfig(level=logging.DEBUG)

# from mhcd_library import *

# Включим оформление в стиле комиксов с сайта xkcd.com
plt.xkcd(scale=3, length=200, randomness=5)


# выбрать значение в квадратных скобках между 0 и 1
regime = ('battle', 'test')[0]
N = 2 if regime == 'test' else None

# Кол-во дней для сбора статистики
days_stat = 7

# Топ сколько участников будем показывать
top_number = 2

# создадим список цветов для графиков
colors = ['#EA047E', '#FF6D28', '#FCE700', '#00F5FF']

# максимальное количество зарубок по оси y на диаграммах
max_ticks = 7

# читаем description из констант
bot_description = texts['bot_mhcd_description_file']
print(bot_description)

# читаем telegram_bot_id из текстового файла
key = 'bot_mhcd_confidential_information_file'
with open(dict_file_names[key], 'r', encoding='utf-8') as f:

    telegram_bot_id = [line.strip('\n') for line in f.readlines()][0]

# Задаем токен
TOKEN = telegram_bot_id

# Создаем объекты бота и диспетчера
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

logging.info('--DEBUG MESSAGE--: Подключение к PostgreSQL-базе данных')
k_0 = 'x_via_y_db'
k_1 = 'big_server_postgres'


def get_engine(key):
    db_config = db_configs[key]
    # Формируем строку соединения с БД.
    connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(
        db_config['user'],
        db_config['pwd'],
        db_config['host'],
        db_config['port'],
        db_config['db'],
    )

    return create_engine(connection_string)


schema = 'dictionaries'
table = 'mhcd_timing'
table_knock_knock = 'mhcd_knock_knock'
table_cooldown = 'mhcd_cooldown'

# Подключаемся к БД.
ls_engines = []
for key in (k_0, k_1)[1:]:
    eng = get_engine(key)
    ls_engines.append(eng)

engine_1 = ls_engines[0]

logging.info(
    '--DEBUG MESSAGE--: Подключение к '
    'PostgreSQL-базе данных установлено',
             )


def is_connected(sleep_that_much_seconds=20):
    try:
        # connect to the host -- tells us if
        # the host is actually reachable
        socket.create_connection(("1.1.1.1", 53))
        return True
    except OSError:
        print('нет подключения. ждём')
        time.sleep(sleep_that_much_seconds)
        return is_connected(sleep_that_much_seconds)


def add_datetime_to_filename(filename):
    """Это функция для добавления даты и времени в
    конец имени файла без изменения расширения.

    Функция принимает на вход имя файла
    (полное или относительное, с расширением).
    """
    # Путь к файлу
    file_path = PurePath(filename)
    # Получаем текущее время
    current_time = dt_.now().strftime('_%Y-%m-%d._%H.%M.%S')
    # Создаем новое имя файла с добавленным временем
    new_stem = f'{file_path.stem}{current_time}'
    # Используем with_stem для создания нового пути с измененным именем файла
    new_filename = file_path.with_stem(new_stem)
    return new_filename


def get_description_for_favorites(date, txt):
    date += timedelta(minutes=2)
    date = date.strftime('%H:%M:%S')  # Форматированное время
    return txt.format(countdown=date)


def get_key_by_val(dict_, val):
    for key, v_cur in dict_.items():
        if v_cur == val:
            return key


# списки ID разрешенных чатов в зависимости от режима работы бота regime
allowed_chat_ids = [
    tg_chat_ids['sandbox'],
    tg_chat_ids['without'],
    tg_chat_ids['me'],
][:N]


# ---------- ВЫЗОВ ИНФОРМАЦИИ О ЧАТ-БОТЕ ----------

@dp.message_handler(commands=['about'])
async def command_start(message: types.Message):
    if message.chat.id in allowed_chat_ids:
        message_from_bot_0 = await bot.send_message(
            message.chat.id,
            get_description_for_favorites(
                message.date,
                texts['bot_mhcd_description_for_favorites_file_0'],
            ),
        )
        message_from_bot_1 = await bot.send_message(
            message.chat.id,
            get_description_for_favorites(
                message.date,
                texts['bot_mhcd_description_for_favorites_file_1'],
            ),
            parse_mode='MarkdownV2',
        )
        message_from_bot_2 = await bot.send_message(
            message.chat.id,
            get_description_for_favorites(
                message.date,
                texts['bot_mhcd_description_for_favorites_file_2'],
            ),
        )
        try:
            await message.delete()
        except:
            print('\nУдаление сообщения не удалось\n')
            await bot.send_message(
                message.chat.id, 'Удаление сообщения не удалось. '
                'Возможно, его уже удалили до меня',
            )
        # Ожидание 2 минут
        await asyncio.sleep(30)  # 120

        # Удаление сообщений через 2 минуты
        try:
            await bot.delete_message(
                message.chat.id, message_from_bot_0.message_id,
            )
        except Exception as e:
            print(f'\nОшибка при удалении сообщения sent_message: {e}\n')
            await bot.send_message(
                message.chat.id, 'Удаление сообщения не удалось. '
                'Возможно, его уже удалили до меня',
            )
        try:
            await bot.delete_message(
                message.chat.id, message_from_bot_1.message_id,
            )
        except Exception as e:
            print(f'\nОшибка при удалении сообщения sent_message: {e}\n')
            await bot.send_message(
                message.chat.id, 'Удаление сообщения не удалось. '
                'Возможно, его уже удалили до меня',
            )
        try:
            await bot.delete_message(
                message.chat.id, message_from_bot_2.message_id,
            )
        except Exception as e:
            print(f'\nОшибка при удалении сообщения sent_message: {e}\n')
            await bot.send_message(
                message.chat.id, 'Удаление сообщения не удалось. '
                'Возможно, его уже удалили до меня',
            )
    else:

        chat_id = message.chat.id
        chat_title = message.chat.title
        request_datetime = message.date
        formatted_datetime = request_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        msg_user_id = message.from_user.id
        msg_username = message.from_user.username
        msg_user_first_name = message.from_user.first_name
        msg_user_last_name = message.from_user.last_name

        df = pd.DataFrame({
            'chat_id': [chat_id],
            'chat_title': [chat_title],
            'request_datetime': [request_datetime],
            'formatted_datetime': [formatted_datetime],
            'msg_user_id': [msg_user_id],
            'msg_username': [msg_username],
            'msg_user_first_name': [msg_user_first_name],
            'msg_user_last_name': [msg_user_last_name],
        })

        # передаём в БД сообщение и связанные с ним данные
        df.to_sql(
            table_knock_knock, con=engine_1, schema=schema, if_exists='append',
        )

        await bot.send_message(
            message.chat.id, 'К сожалению, вы не '
            'авторизованы для взаимодействия со мной.',
        )
        print(
            f'\n{message.chat.id}, к сожалению, вы не '
            'авторизованы для взаимодействия со мной.\n',
        )
        return

with engine_1.connect() as connection:

    # извлекаем сведения о таблицах из БД
    df_tables_info = pd.read_sql_table(
        'df_tables_info', connection, schema=schema,
    )
dict_dictname_2_command = {}
for df_name in df_tables_info['df_name']:
    dict_dictname_2_command[df_name] = (
        df_tables_info.loc[df_tables_info[
            'df_name'
            ] == df_name, 'main_part_of_file_name'].values[0],
        df_tables_info.loc[df_tables_info[
            'df_name'
            ] == df_name, 'table_id'].values[0],
    )
# Список таблиц для которых вы хотите создать команды
ls_sql_dictionaries = [
    key for key in dict_dictname_2_command.keys()
]

# --------- ЗАПРОС НА ПОЛУЧЕНИЕ ОДНОГО ИЗ СЛОВАРЕЙ В ВИДЕ EXCEL ФАЙЛА ---------


# Общая функция для обработки команд
async def handle_get_table(message: types.Message, dict_name: str):

    try:
        await message.delete()
    except:
        print(f'\nУдаление сообщения не удалось\n')
        await bot.send_message(
            message.chat.id, 'Удаление сообщения не удалось. '
            'Возможно, его уже удалили до меня',
        )

    if message.chat.id in allowed_chat_ids:

        await bot.send_message(
            message.from_user.id, 'Вы запросили данные из '
            f'словаря "{dict_name}"',
        )

        try:

            k = 'request_get_sql_dictionary'
            df = pd.read_sql_query(sql_requests[k].format(
                schema=schema,
                tablename=dict_name,
            ), con=engine_1)

            length = len(df)
            await bot.send_message(
                message.from_user.id,
                f'Длина словаря "{dict_name}" составляет: {length} строк.\n'
                'Начинаю передавать словарь личным сообщением:'
            )

            # путь к файлу, который нужно отправить
            fn_xlsx = rf'..\temp\{dict_name}.xlsx'
            # сохраняем извлечённый из БД словарь во временный файл
            df.to_excel(fn_xlsx, index=False, freeze_panes=(1, 0))
            # создаем объект InputFile, передавая ему путь к файлу
            document = InputFile(fn_xlsx)
            # отправляем документ пользователю
            await bot.send_document(message.from_user.id, document=document)
            # удаление файла после отправки
            os.remove(fn_xlsx)

        except:

            await bot.send_message(
                message.from_user.id,
                'Не удалось выполнить операцию. Скорее '
                f'всего, словаря "{dict_name}" ещё нет в БД SQL.',
            )

    else:

        chat_id = message.chat.id
        chat_title = message.chat.title
        request_datetime = message.date
        formatted_datetime = request_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        msg_user_id = message.from_user.id
        msg_username = message.from_user.username
        msg_user_first_name = message.from_user.first_name
        msg_user_last_name = message.from_user.last_name

        df = pd.DataFrame({
            'chat_id': [chat_id],
            'chat_title': [chat_title],
            'request_datetime': [request_datetime],
            'formatted_datetime': [formatted_datetime],
            'msg_user_id': [msg_user_id],
            'msg_username': [msg_username],
            'msg_user_first_name': [msg_user_first_name],
            'msg_user_last_name': [msg_user_last_name],
        })

        # передаём в БД сообщение и связанные с ним данные
        df.to_sql(
            table_knock_knock, con=engine_1,
            schema=schema, if_exists='append', index=False,
        )

        await bot.send_message(
            message.chat.id, 'А, не повезло, ваш чат не '
            'авторизован для взаимодействия со мной.',
        )


# Динамически добавляем обработчики команд
for sql_dictionary in ls_sql_dictionaries:
    dp.register_message_handler(
        lambda message,
        table=sql_dictionary: handle_get_table(message, table),
        commands=[f'get_{sql_dictionary}'],
    )


# ---------- ВЫЗОВ ИНФО О ТОП-3 АКТИВНЫХ УЧАСТНИКАХ ЧАТА ----------

@dp.message_handler(commands=['who_is_your_daddy'])
async def command_daddy(message: types.Message):

    try:
        await message.delete()
    except:
        print(f'\nУдаление сообщения не удалось\n')
        await bot.send_message(
            message.chat.id,
            'Удаление сообщения не удалось. Возможно, его уже удалили до меня',
        )

    if message.chat.id in allowed_chat_ids:

        start_time = message.date - timedelta(days=days_stat)

        key = 'request_daddy'
        df = pd.read_sql_query(sql_requests[key].format(
            schema=schema,
            tablename=table,
            chat_id=message.chat.id,
            now_minus_one_week=start_time,
        ), con=engine_1)

        str_start_time = start_time.strftime('%Y-%m-%d %H:%M:%S.%f')
        str_end_time = message.date.strftime('%Y-%m-%d %H:%M:%S.%f')

        df_grouped = df.groupby(
            ['user_id', 'username'],
            as_index=False).agg(total=('count', 'sum'),
        )

        xbar = df_grouped['username'].tolist()[:top_number]
        hbar = df_grouped['total'].tolist()[:top_number]

        # реальный размер топа
        is_min = len(xbar)

        # Очистка полотна
        plt.clf()

        # # Создание графика с чёрным фоном
        # plt.figure(facecolor='black')

        # Создание диаграммы
        plt.bar(xbar, hbar, color=colors)

        # Устанавливаем метки на оси y
        yticks = [int(y) for y in range(0, max(hbar) + 1)]
        yticks = yticks[::(len(yticks) // (max_ticks - 1)) + 1]
        plt.yticks(yticks, yticks)

        # # Добавление зелёных меток по осям
        # plt.tick_params(axis='x', colors='green')
        # plt.tick_params(axis='y', colors='green')

        # # Добавление линий сетки
        # plt.grid(color='green')

        # Добавление заголовка
        plt.title(
            f'Топ-{min(top_number, is_min)} активных участника чата\n'
            f'с {str_start_time[:-10]} по {str_end_time[:-10]}',
        )

        # Добавление названий осей
        plt.xlabel('Имя участника чата', color='k')
        plt.ylabel('Кол-во сообщений, шт', color='k')

        plt.tight_layout()

        # Сохранение графика как файла изображения
        plt.savefig(f'plot_{message.chat.id}.png')

        # Создание объекта Photo для отправки
        photo = types.InputFile(f'plot_{message.chat.id}.png')

        # Отправка фото через бота Телеграм
        await bot.send_photo(message.chat.id, photo)

        # Удаление файла изображения после отправки
        os.remove(f'plot_{message.chat.id}.png')

        # plt.show()

        df_grouped = df.groupby(['type_of_msg_content', 'user_id', 'username'], as_index=False).agg(total=('count', 'sum'))

        df_grouped['total'] = df_grouped['total'].astype('int')

        for type_of_msg_content in (df_grouped['type_of_msg_content']
                                    .unique().tolist()):

            df_grouped_sliced = df_grouped.loc[df_grouped['type_of_msg_content'] == type_of_msg_content]

            xbar = df_grouped_sliced['username'].tolist()[:top_number]
            hbar = df_grouped_sliced['total'].tolist()[:top_number]

            # реальный размер топа
            is_min = len(xbar)

            # Очистка полотна
            plt.clf()

            # Создание диаграммы
            plt.bar(xbar, hbar, color=colors)

            # Устанавливаем метки на оси y
            yticks = [int(y) for y in range(0, max(hbar) + 1)]
            yticks = yticks[::(len(yticks) // (max_ticks - 1)) + 1]
            plt.yticks(yticks, yticks)

            # Добавление заголовка
            plt.title(
                f'Топ-{min(top_number, is_min)} отправителя контента '
                f'типа {type_of_msg_content} в чат\nс '
                f'{str_start_time[:-10]} по {str_end_time[:-10]}',
            )

            # Добавление названий осей
            plt.xlabel('Имя участника чата', color='k')
            plt.ylabel(f'Кол-во {type_of_msg_content}, шт', color='k')

            plt.tight_layout()

            # Сохранение графика как файла изображения
            plt.savefig(f'plot_{message.chat.id}_{type_of_msg_content}.png')

            # Создание объекта Photo для отправки
            photo = types.InputFile(
                f'plot_{message.chat.id}_{type_of_msg_content}.png',
            )

            # Отправка фото через бота Телеграм
            await bot.send_photo(message.chat.id, photo)

            # Удаление файла изображения после отправки
            os.remove(f'plot_{message.chat.id}_{type_of_msg_content}.png')

            # plt.show()

    else:

        chat_id = message.chat.id
        chat_title = message.chat.title
        request_datetime = message.date
        formatted_datetime = request_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        msg_user_id = message.from_user.id
        msg_username = message.from_user.username
        msg_user_first_name = message.from_user.first_name
        msg_user_last_name = message.from_user.last_name

        df = pd.DataFrame({
            'chat_id': [chat_id],
            'chat_title': [chat_title],
            'request_datetime': [request_datetime],
            'formatted_datetime': [formatted_datetime],
            'msg_user_id': [msg_user_id],
            'msg_username': [msg_username],
            'msg_user_first_name': [msg_user_first_name],
            'msg_user_last_name': [msg_user_last_name],
        })

        # передаём в БД сообщение и связанные с ним данные
        df.to_sql(
            table_knock_knock, con=engine_1, schema=schema, if_exists='append',
        )

        await bot.send_message(
            message.chat.id, 'А, не повезло, ваш чат '
            'не авторизован для взаимодействия со мной.',
        )


# ---------- ВЫЗОВ ИНФО ПО СТАТИСТИКЕ ВРЕМЕНИ АКТИВНОСТИ ЧАТА ----------

@dp.message_handler(commands=['no_time_to_waste'])
async def command_waste(message: types.Message):

    try:
        await message.delete()
    except:
        print('\nУдаление сообщения не удалось\n')
        await bot.send_message(
            message.chat.id, 'Удаление сообщения не '
            'удалось. Возможно, его уже удалили до меня',
        )

    if message.chat.id in allowed_chat_ids:

        start_time = message.date - timedelta(days=days_stat)

        k = 'request_waste_0'
        df = pd.read_sql_query(sql_requests[k].format(
            schema=schema,
            tablename=table,
            chat_id=message.chat.id,
            now_minus_one_week=start_time,
        ), con=engine_1)

        str_start_time = start_time.strftime('%Y-%m-%d %H:%M:%S.%f')
        str_end_time = message.date.strftime('%Y-%m-%d %H:%M:%S.%f')

        df_grouped = df.groupby(['dow'], as_index=False).agg(
            total=('count', 'sum'),
        )

        df_grouped['dow'] = df_grouped['dow'].astype(int)
        df_grouped['dow'] = df_grouped['dow'].replace(dict_dows)

        xbar = df_grouped['dow'].tolist()[:]
        hbar = df_grouped['total'].tolist()[:]
        if 'вс' == xbar[0]:
            xbar = xbar[1:] + [xbar[0]]
            hbar = hbar[1:] + [hbar[0]]

        # Очистка полотна
        plt.clf()

        # Создание диаграммы
        plt.bar(xbar, hbar, color=colors)

        # Устанавливаем метки на оси y
        yticks = [int(y) for y in range(0, max(hbar) + 1)]
        yticks = yticks[::(len(yticks) // (max_ticks - 1)) + 1]
        plt.yticks(yticks, yticks)

        # Добавление заголовка
        plt.title(
            'Активность чата по дням недели\nс '
            f'{str_start_time[:-10]} по {str_end_time[:-10]}',
        )

        # Добавление названий осей
        plt.xlabel('День недели', color='k')
        plt.ylabel('Кол-во сообщений, шт', color='k')

        plt.tight_layout()

        # Сохранение графика как файла изображения
        plt.savefig(
            f"plot_{message.chat.id}_0_{str_end_time.replace(':', '_')}.png",
        )

        # Создание объекта Photo для отправки
        photo = types.InputFile(
            f"plot_{message.chat.id}_0_{str_end_time.replace(':', '_')}.png",
        )

        # Отправка фото через бота Телеграм
        await bot.send_photo(message.chat.id, photo)

        # Удаление файла изображения после отправки
        os.remove(
            f"plot_{message.chat.id}_0_{str_end_time.replace(':', '_')}.png",
        )

        # plt.show()

        if len(xbar) < 7:
            await bot.send_message(
                message.chat.id, texts['bot_alex_v_13_text_about_dates'],
            )

        k = 'request_waste_1'
        df = pd.read_sql_query(sql_requests[k].format(
            schema=schema,
            tablename=table,
            chat_id=message.chat.id,
            now_minus_one_week=start_time,
        ), con=engine_1)

        df_grouped = (df
                      .groupby(['hour'], as_index=False)
                      .agg(total=('count', 'sum')))

        df_grouped['hour'] = df_grouped['hour'].astype(int)

        xbar = df_grouped['hour'].tolist()[:]
        hbar = df_grouped['total'].tolist()[:]

        # Очистка полотна
        plt.clf()

        # Создание диаграммы
        plt.bar(xbar, hbar, color=colors)

        # Устанавливаем метки на оси y
        yticks = [int(y) for y in range(0, max(hbar) + 1)]
        yticks = yticks[::(len(yticks) // (max_ticks - 1)) + 1]
        plt.yticks(yticks, yticks)

        # Устанавливаем метки на оси x
        xticks = [int(x) for x in range(0, 24 + 1)]
        # xticks = xticks[::(len(xticks) // (max_ticks - 1)) + 1]
        plt.xticks(xticks, xticks)

        # Добавление заголовка
        plt.title(
            'Усреднённая активность чата по часам\nс '
            f'{str_start_time[:-10]} по {str_end_time[:-10]}',
        )

        # Добавление названий осей
        plt.xlabel('Час суток', color='k')
        plt.ylabel(f'Кол-во сообщений, шт', color='k')

        plt.tight_layout()

        # Сохранение графика как файла изображения
        plt.savefig(
            f"plot_{message.chat.id}_1_{str_end_time.replace(':', '_')}.png",
        )

        # Создание объекта Photo для отправки
        photo = types.InputFile(
            f"plot_{message.chat.id}_1_{str_end_time.replace(':', '_')}.png",
        )

        # Отправка фото через бота Телеграм
        await bot.send_photo(message.chat.id, photo)

        # Удаление файла изображения после отправки
        os.remove(
            f"plot_{message.chat.id}_1_{str_end_time.replace(':', '_')}.png",
        )

        # plt.show()

    else:

        chat_id = message.chat.id
        chat_title = message.chat.title
        request_datetime = message.date
        formatted_datetime = request_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        msg_user_id = message.from_user.id
        msg_username = message.from_user.username
        msg_user_first_name = message.from_user.first_name
        msg_user_last_name = message.from_user.last_name

        df = pd.DataFrame({
            'chat_id': [chat_id],
            'chat_title': [chat_title],
            'request_datetime': [request_datetime],
            'formatted_datetime': [formatted_datetime],
            'msg_user_id': [msg_user_id],
            'msg_username': [msg_username],
            'msg_user_first_name': [msg_user_first_name],
            'msg_user_last_name': [msg_user_last_name],
        })

        # передаём в БД сообщение и связанные с ним данные
        df.to_sql(
            table_knock_knock, con=engine_1, schema=schema, if_exists='append',
        )

        await bot.send_message(
            message.chat.id, 'А, не повезло, ваш чат не '
            'авторизован для взаимодействия со мной.',
        )


# ---------- Обновление словарей в БД SQL ----------

@dp.message_handler(content_types=[
    types.ContentType.DOCUMENT,
    types.ContentType.VIDEO,
    types.ContentType.PHOTO,
    types.ContentType.STICKER,
    types.ContentType.VOICE,
    types.ContentType.LOCATION,
    types.ContentType.CONTACT,
])
async def handle_all_messages(message: types.Message):

    if message.chat.id in allowed_chat_ids:

        # Проверяем, есть ли у сообщения атрибут caption
        if hasattr(message, 'caption') and message.caption:
            # Если атрибут caption существует и не пустой, работаем с ним
            txt_msg_got_from_caption = True
            txt_msg = message.caption
        else:
            # Если атрибут caption отсутствует или пустой, оставляем пустым
            txt_msg_got_from_caption = False
            txt_msg = np.nan

        # если загруженный контент - документ
        if message.content_type == types.ContentType.DOCUMENT:
            # и если его имя файла совпадает с одной из масок в словаре
            for key, v in dict_regexes.items():
                if re.search(v, message.document.file_name, flags=re.I):

                    print(f'\n{re.match(v, message.document.file_name, flags=re.I).group(1)}\n')
                    # main_part_of_file_name = re.match(v, message.document.file_name, flags=re.I).group(1)
                    fn = re.match(v, message.document.file_name, flags=re.I).group(0)

                    # добавляем к имени файла со словарём дату,
                    # время и id отправителя
                    xlsx_file = add_datetime_to_filename(PurePath(
                        rf'..\backup\{PurePath(message.document.file_name).stem}_by_'
                        rf'{message.from_user.id}{PurePath(message.document.file_name).suffix}',
                    ))

                    pkey_val = dict_dictname_2_command[key][1]

                    ls_datetime_of_upload = re.split(r'-|\._?', str(xlsx_file)[-25:-5])
                    datetime_of_upload = dt_(
                        *[int(_) for _ in ls_datetime_of_upload],
                    )

                    query_1_raw = '''
UPDATE {schema}."{table_name}"
    SET "{column_name_0}"='{column_val_0}'
        , "{column_name_1}"='{column_val_1}'
    WHERE "{pkey_name}"='{pkey_val}'
;
                    '''
                    query_1 = query_1_raw.format(
                        schema=schema,
                        table_name='df_tables_info',
                        column_name_0='last_update_timedate',
                        # column_val_0=datetime_of_upload.strftime(format='%Y-%m-%d %H:%M:%S'),
                        column_val_0=datetime_of_upload,
                        column_name_1='last_sender',  # по факту - имя файла словаря
                        column_val_1=fn,
                        pkey_name='table_id',
                        pkey_val=pkey_val,
                    )

                    # сохраняем на диск
                    await message.document.download(destination=xlsx_file)
                    # звгружаем в фрейм
                    df = pd.read_excel(xlsx_file)

                    with engine_1.connect() as connection:
                        # читаем словарь названий
                        k = 'request_read_dict_names'
                        df_dict = pd.read_sql_query(sql_requests[k].format(
                            schema=schema,
                            tablename='df_names',
                        ), con=connection)
                        connection.execute(text(query_1))
                        # Если используете явные транзакции
                        connection.commit()

                    # # проверяем на дубликаты в ключе
                    # assert df_dict.drop_duplicates().duplicated(subset=['rus_name']).sum() == 0, 'В словаре df_names есть дублирующиеся ключи'
                    # превращаем его датафрейм в словарь
                    dict_names = df_dict.set_index('rus_name')['eng_name'].to_dict()
                    # переименовываем колонки с русского на английский
                    df.rename(columns=dict_names, inplace=True)
                    # залолняем данными соответствующую ему таблицу в БД SQL (с перезаписью)
                    df.to_sql(key, engine_1, 'dictionaries', if_exists='replace', index=False)
                    # выводим сообщение об успешном завершении операции
                    await message.reply('Данные словаря успешно обновлены')

                    # pass

        chat_id = message.chat.id
        chat_alias = get_key_by_val(tg_chat_ids, message.chat.id)
        chat_title = message.chat.title
        msg_user_id = message.from_user.id
        msg_username = message.from_user.username
        msg_user_first_name = message.from_user.first_name
        msg_user_last_name = message.from_user.last_name
        type_of_msg_content = message.content_type
        msg_time = message.date  # Время получения (отправки) сообщения
        print('\n', msg_time, '\n', txt_msg, '\n', )
        # Форматированное время
        formatted_time = msg_time.strftime('%Y-%m-%d %H:%M:%S.%f')

        df = pd.DataFrame({
            'date_time': [formatted_time],
            'chat_id': [chat_id],
            'chat_alias': [chat_alias],
            'chat_title': [chat_title],
            'user_id': [msg_user_id],
            'username': [msg_username],
            'first_name': [msg_user_first_name],
            'last_name': [msg_user_last_name],
            'message': [txt_msg],
            'type_of_msg_content': [type_of_msg_content],
            'txt_msg_got_from_caption': [txt_msg_got_from_caption],
        })

        # передаём в БД сообщение и связанные с ним данные
        df.to_sql(table, con=engine_1, schema=schema, if_exists='append')

    elif message.chat.id not in allowed_chat_ids:

        chat_id = message.chat.id
        chat_title = message.chat.title
        request_datetime = message.date
        formatted_datetime = request_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        msg_user_id = message.from_user.id
        msg_username = message.from_user.username
        msg_user_first_name = message.from_user.first_name
        msg_user_last_name = message.from_user.last_name

        df = pd.DataFrame({
            'chat_id': [chat_id],
            'chat_title': [chat_title],
            'request_datetime': [request_datetime],
            'formatted_datetime': [formatted_datetime],
            'msg_user_id': [msg_user_id],
            'msg_username': [msg_username],
            'msg_user_first_name': [msg_user_first_name],
            'msg_user_last_name': [msg_user_last_name],
        })

        # передаём в БД сообщение и связанные с ним данные
        df.to_sql(
            table_knock_knock, con=engine_1, schema=schema, if_exists='append',
        )

        await bot.send_message(
            message.chat.id, 'А, не повезло, ваш чат не '
            'авторизован для взаимодействия со мной.',
        )

    else:
        print(
            f'--DEBUG MESSAGE--: Чат с id {message.chat.id} '
            'совершил действие, не подходящее ни под одно из условий',
        )

    return


# --- функция для парсинга текстовых сообщений из авторизованных групп ---

@dp.message_handler(content_types=types.ContentType.TEXT)
async def handle_text(message: types.Message):

    if message.chat.id in allowed_chat_ids:

        # Проверяем, есть ли у сообщения атрибут caption
        if hasattr(message, 'caption') and message.caption:
            # Если атрибут caption существует и не пустой, работаем с ним
            txt_msg_got_from_caption = True
            txt_msg = message.caption
        else:
            # Если атрибут caption отсутствует или пустой,
            # тащим сообщение из текста
            txt_msg_got_from_caption = False
            txt_msg = message.text

        chat_id = message.chat.id
        chat_alias = get_key_by_val(tg_chat_ids, message.chat.id)
        chat_title = message.chat.title
        msg_user_id = message.from_user.id
        msg_username = message.from_user.username
        msg_user_first_name = message.from_user.first_name
        msg_user_last_name = message.from_user.last_name
        type_of_msg_content = message.content_type
        msg_time = message.date  # Время получения (отправки) сообщения
        print('\n', msg_time, '\n', txt_msg, '\n', )
        # Форматированное время
        formatted_time = msg_time.strftime('%Y-%m-%d %H:%M:%S.%f')

        df = pd.DataFrame({
            'date_time': [formatted_time],
            'chat_id': [chat_id],
            'chat_alias': [chat_alias],
            'chat_title': [chat_title],
            'user_id': [msg_user_id],
            'username': [msg_username],
            'first_name': [msg_user_first_name],
            'last_name': [msg_user_last_name],
            'message': [txt_msg],
            'type_of_msg_content': [type_of_msg_content],
            'txt_msg_got_from_caption': [txt_msg_got_from_caption],
        })

        # передаём в БД сообщение и связанные с ним данные
        df.to_sql(table, con=engine_1, schema=schema, if_exists='append')

    elif message.chat.id not in allowed_chat_ids:

        chat_id = message.chat.id
        chat_title = message.chat.title
        request_datetime = message.date
        formatted_datetime = request_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        msg_user_id = message.from_user.id
        msg_username = message.from_user.username
        msg_user_first_name = message.from_user.first_name
        msg_user_last_name = message.from_user.last_name

        df = pd.DataFrame({
            'chat_id': [chat_id],
            'chat_title': [chat_title],
            'request_datetime': [request_datetime],
            'formatted_datetime': [formatted_datetime],
            'msg_user_id': [msg_user_id],
            'msg_username': [msg_username],
            'msg_user_first_name': [msg_user_first_name],
            'msg_user_last_name': [msg_user_last_name],
        })

        # передаём в БД сообщение и связанные с ним данные
        df.to_sql(
            table_knock_knock, con=engine_1, schema=schema, if_exists='append',
        )

        await bot.send_message(
            message.chat.id, 'А, не повезло, ваш чат '
            'не авторизован для взаимодействия со мной.',
        )

    else:
        print(
            f'--DEBUG MESSAGE--: Чат с id {message.chat.id} '
            'совершил действие, не подходящее ни под одно из условий',
        )

    return


if __name__ == '__main__':
    # для автозагрузки: ждём, пока установится
    # сталбильное подключение к интернету
    if is_connected():
        logging.info('--DEBUG MESSAGE--: Бот вышел в онлайн...')
        asyncio.run(dp.start_polling())
