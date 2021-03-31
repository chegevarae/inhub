# ETL-script для предобработки данных перед подачей их в Tableau

import numpy as np
import pandas as pd
import re
from transliterate import translit, get_available_language_codes

# Функции для обработки данных
import functions as fn


# Загружаем датасет
df = pd.read_csv('../data/wp_cn_requests_price.csv')
# Удаляем дубликаты по id_session
df = df.drop_duplicates(subset='id_session', keep="first")

# Удаление ненужных данных
columns = ['id', 'id_session']
df = fn.drop_col(df, columns)

# Убираем 2020 год (там одни тесты)
df = df[df['created'].str.contains('2021.*')]

# Переименование столбцов
df.columns = fn.get_translite(df.columns, 'st_')

# Обработка выбросов
df.loc[df['st_amount'] > 2000, 'st_amount'] = df['st_amount'].median()

# Корректировка значений
df.loc[df['st_id_ym'] == '', 'st_id_ym'] = 'undefined'
df.loc[df['st_id_ga'] == '', 'st_id_ga'] = 'undefined'
df.loc[df['st_id_ym'].isnull(), 'st_id_ym'] = 'undefined'
df.loc[df['st_id_ga'].isnull(), 'st_id_ga'] = 'undefined'
df.loc[df['st_price'] == 'По запросу', 'st_price'] = 0
df.loc[df['st_price'] == 0.0, 'st_price'] = df['st_price'].median()

# Поиск и замена по регулярке
df.loc[df['st_location'].str.contains("’"), 'st_location'] = df['st_location'].str.replace("’", "")
df.loc[df['st_location'].str.contains(r"\\'"), 'st_location'] = df['st_location'].str.replace(r"\\'", "")

# Заменим название направления типа "Другое" на название корневой отрасли
df.loc[df['st_direction'] == 'Другое', 'st_direction'] = df['st_branch']

# Приведение типов
df = fn.astype_col(df, ['st_amount'], coltype='uint8')
df = fn.astype_col(df, ['st_price'], coltype='float64')

# Сохраняем в файл
df.to_csv('../data/td_site.csv', index=False, encoding='utf-8', sep=';')
