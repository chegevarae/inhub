# ETL-script для предобработки данных перед подачей их в Tableau

import numpy as np
import pandas as pd

# Загружаем датасеты без 1-й строки и делаем переиндексацию с удалением столбца index
df1 = pd.read_csv('hubex.ru-ClientID-Sources-Behavior.csv')[1:].reset_index(drop=True) # Дата
df2 = pd.read_csv('hubex.ru-ClientID-Sources-Behavior (1).csv')[1:].reset_index(drop=True) # Город
# Удаляем дубликаты по ClientID
df1 = df1.drop_duplicates(subset='ClientID', keep="first")
df2 = df2.drop_duplicates(subset='ClientID', keep="first")

# Производим слияние датасетов
df = pd.merge(df1, df2, how='outer')

# # Удаление ненужных данных
df = df.drop('Посетители', axis=1)
# # Переименование столбцов
df.columns=['ClientID', 'First traffic source', 'First click sites', 'First search engine', 'UTM source', 'UTM content', 
            'Date', 'Sessions', 'Bounces', 'Viewing depth', 'Time site', 'New visitors', 'Days between sessions', 
            'Returned 1 day', 'Returned 2-7 days', 'Achieving any goal', 'City']

# Корректировка значений
df.loc[df['First click sites'] == 'Не определено', 'First click sites'] = np.nan
df.loc[df['First search engine'] == 'Не определено', 'First search engine'] = np.nan
df.loc[df['UTM source'] == 'Не определено', 'UTM source'] = np.nan
df.loc[df['UTM content'] == 'Не определено', 'UTM content'] = np.nan
df.loc[df['Days between sessions'] == 'Не определено', 'Days between sessions'] = np.nan

# Удаление "лишних" пользователей
df = df.loc[(df['Sessions'] <= 35) & (df['Achieving any goal'] <= 100)]
df = df.loc[(df['ClientID'] != '')]

# Слияние столбцов
df['Source'] = df[df.columns[2:6]].apply(lambda x: ': '.join(x.dropna().astype(str)), axis=1)

# Удаляем ненужные столбцы
df = df.drop('First click sites', axis=1)
df = df.drop('First search engine', axis=1)
df = df.drop('UTM source', axis=1)
df = df.drop('UTM content', axis=1)

# Меняем порядок столбцов
df = df[['ClientID', 'First traffic source', 'Source', 'City', 'Date', 'Sessions', 'Bounces', 'Viewing depth', 
         'Time site', 'New visitors', 'Days between sessions', 'Returned 1 day', 'Returned 2-7 days', 
         'Achieving any goal']].reset_index(drop=True)

# Корректируем источники
df['Source'] = df['Source'].str.lower() # Переводим все нижний регистр
df.loc[(df['Source'] == 'яндекс') | (df['Source'] == 'yandex.ru'), 'Source'] = 'yandex'
df.loc[(df['Source'] == '') | (df['Source'].isnull()), 'Source'] = 'другое' # Убираем Null и пустоты
df = df[~df['Source'].str.contains('([0-9]{1,3}[\.]){3}[0-9]{1,3}')] # Убираем ip-адреса
df.loc[df['Source'] == 'мобильные%сотрудники', 'Source'] = 'другое'
df.loc[df['Source'] == 'старт за 2 дня', 'Source'] = 'другое'
df.loc[df['Source'] == 'яндекс: serp', 'Source'] = 'yandex: serp'

# Корректировка значений
df.loc[df['Date'].isnull(), 'Date'] = '2018-2019'
df = df[df['Date'].str.contains('202.*')]

# Добавляем стоимость клика по каждому каналу
df.loc[df['First traffic source'] == 'Прямые заходы', 'Cost'] = 0
df.loc[df['First traffic source'] == 'Переходы по рекламе', 'Cost'] = 15
df.loc[df['First traffic source'] == 'Переходы из поисковых систем', 'Cost'] = 48.5
df.loc[df['First traffic source'] == 'Внутренние переходы', 'Cost'] = 0
df.loc[df['First traffic source'] == 'Переходы по ссылкам на сайтах', 'Cost'] = 0
df.loc[df['First traffic source'] == 'Переходы из социальных сетей', 'Cost'] = 0
df.loc[df['First traffic source'] == 'Переходы с почтовых рассылок', 'Cost'] = 2
df.loc[df['First traffic source'] == 'Переходы из рекомендательных систем', 'Cost'] = 0
df.loc[df['First traffic source'] == 'Переходы из мессенджеров', 'Cost'] = 0
df.loc[df['First traffic source'] == 'Не определено', 'Cost'] = 0

# Сохраняем в файл индустрии
df.to_csv('TD-Metrika.csv', index=False, encoding='utf-8', sep=';')
