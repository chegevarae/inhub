# ETL-script для предобработки данных перед подачей их в Tableau

import numpy as np
import pandas as pd

# Функция приведения типов
def astype_col(df, colgroup, coltype):
    for colname in colgroup:
        df[colname] = df[colname].astype(coltype)
    return df

# Загружаем данные
df = pd.read_csv('wp_cn_requests_price.csv')

# Удаление ненужных данных
df = df.drop('id', axis=1)
# Переименование столбцов
df.columns=['SessionID', 'YMID', 'GAID', 'Date', 'Location', 'Industry', 'Direction', 'Licenses', 'Budget']
# Убираем 2020 год (там одни тесты)
df = df[df['Date'].str.contains('2021.*')]

# Обработка выбросов
df.loc[df['Licenses'] > 2000, 'Licenses'] = df['Licenses'].median()

# Корректировка значений
df.loc[df['YMID'] == '', 'YMID'] = np.nan
df.loc[df['GAID'] == '', 'GAID'] = np.nan
df.loc[df['Budget'] == 'По запросу', 'Budget'] = 0
df.loc[df['Budget'] == 0.0, 'Budget'] = df['Budget'].median()

# Заменим название направление типа "Другое" на название корневой отрасли
df.loc[df['Direction'] == 'Другое', 'Direction'] = df['Industry']

# Приведение типов
df = astype_col(df, ['Date'], coltype='datetime64')
df = astype_col(df, ['Licenses'], coltype='uint8')
df = astype_col(df, ['Budget'], coltype='float64')

# Удалим дубликаты по SessionID через контроль времени в поле Дата
df = df.drop_duplicates(subset='SessionID', keep="first")

# Скорректируем дату для более удобной группировки данных
df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

# Сохраняем в файл индустрии
df.to_csv('TD-Site.csv', index=False, encoding='utf-8', sep=';')
