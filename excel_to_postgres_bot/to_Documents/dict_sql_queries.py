#!/usr/bin/python
# -*- coding: utf-8 -*-

dict_sql_queries = {
    'get_tables_info_4_clmns_raw': '''
SELECT "{pkey_name}"
    , "{column_name}"
    , "last_update_timedate"
    , "last_sender"
FROM {schema}."{table_name}"
WHERE "{pkey_name}" IN ({sql_formatted_list})
;
''',

    'set_val_raw': '''
UPDATE {schema}."{table_name}"
    SET "{column_name}"={column_val}
    WHERE "{pkey_name}"='{pkey_val}'
;
''',

    # запрос для получения имён столбцов и типов данных
    # в таблице `table_name` (при её отсутствии вернётся пустой df)
    'get_tables_info_2_clmns_raw': '''
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = '{table_schema}'
AND table_name = '{table_name}'
;
''',

    # добавление первичного ключа (допускается составной)
    'add_primary_key_raw': '''
ALTER TABLE {table_schema}."{table_name}"
ADD CONSTRAINT {table_name}_pkey PRIMARY KEY ({csv})
;
''',

    # Создание индекса (после сохранения данных)
    'create_index_raw': '''
CREATE INDEX idx_{table_name}_{column_name}
ON {table_schema}."{table_name}" ({column_name})
;
''',

    # Выдача доступа роли (пользователю) к схеме
    'grant_usage_on_schema_raw': '''
GRANT USAGE ON SCHEMA {table_schema} TO {role};
;
''',

    # Выдача прав на чтение таблицы
    'grant_reading_privileges_raw': '''
GRANT UPDATE, TRIGGER, SELECT, REFERENCES ON TABLE {table_schema}."{table_name}" TO {role}
;
''',

    # Отзыв прав на чтение таблицы
    'revoke_reading_privileges_raw': '''
REVOKE UPDATE, TRIGGER, SELECT, REFERENCES ON TABLE {table_schema}."{table_name}" FROM {role}
;
''',

    # Получение списка имён не-системных схем базы данных
    'get_schema_names_raw': '''
SELECT schema_name
FROM information_schema.schemata
WHERE TRUE
    AND schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
;
''',

    # Получение списка имён таблиц схемы
    'get_table_names_raw': '''
SELECT table_name
FROM information_schema.tables
WHERE table_schema = '{schema_name}'
    AND table_type = 'BASE TABLE'
;
''',

    # создание партицированной таблицы-родителя
    '': '''

''',

}



# sql_formatted_list = f"{','.join([f"'{_}'" for _ in ls_dict_numbers])}"