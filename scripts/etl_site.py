# ETL-script для предобработки данных перед подачей их в Tableau

import numpy as np
import pandas as pd
import re
from transliterate import translit, get_available_language_codes

# Функция приведения типов
def astype_col(df, colgroup, coltype):
    for colname in colgroup:
        df[colname] = df[colname].astype(coltype)
    return df

# Функция удаления столбцов
def drop_columns(df, columns):
    for column in columns:
        df = df.drop(column, axis=1)  
    return df

# Функция транслитерации
def get_translite(columns):
    lst = []
    prefix = 'ST_'
    for column in columns:
        r = translit(prefix + column, 'ru', reversed=True).lower()
        r = re.sub("\'|\(|\)|:", "", r)
        r = re.sub("\ |-|‑", "_", r)
        lst.append(r)
    return lst

# Функция получение имен столбцов с индексом
def get_idx_columns(columns):
    lst = []
    for index, value in enumerate(columns):
        print(index, value)
        lst.append(index)
    print(lst)


# Загружаем датасет без 1-й строки и делаем переиндексацию с удалением столбца index
df = pd.read_csv('../data/wp_cn_requests_price.csv')
# Удаляем дубликаты по id_session
df = df.drop_duplicates(subset='id_session', keep="first")

# Удаление ненужных данных
columns = ['id', 'id_session']
df = drop_columns(df, columns)

# Убираем 2020 год (там одни тесты)
df = df[df['created'].str.contains('2021.*')]

# Переименование столбцов
df.columns = get_translite(df.columns)

# Обработка выбросов
df.loc[df['st_amount'] > 2000, 'st_amount'] = df['st_amount'].median()

# Корректировка значений
df.loc[df['st_id_ym'] == '', 'st_id_ym'] = 'undefined'
df.loc[df['st_id_ga'] == '', 'st_id_ga'] = 'undefined'
df.loc[df['st_id_ym'].isnull(), 'st_id_ym'] = 'undefined'
df.loc[df['st_id_ga'].isnull(), 'st_id_ga'] = 'undefined'
df.loc[df['st_price'] == 'По запросу', 'st_price'] = 0
df.loc[df['st_price'] == 0.0, 'st_price'] = df['st_price'].median()

# Заменим название направления типа "Другое" на название корневой отрасли
df.loc[df['st_direction'] == 'Другое', 'st_direction'] = df['st_branch']

# Приведение типов
df = astype_col(df, ['st_created'], coltype='datetime64')
df = astype_col(df, ['st_amount'], coltype='uint8')
df = astype_col(df, ['st_price'], coltype='float64')

# Скорректируем дату для более удобной группировки данных
df['st_created'] = pd.to_datetime(df['st_created']).dt.strftime('%Y-%m-%d')

# Сохраняем в файл
df.to_csv('../data/td_site.csv', index=False, encoding='utf-8', sep=';')
