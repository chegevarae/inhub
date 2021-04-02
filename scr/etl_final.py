# ETL-script для предобработки данных перед подачей их в Tableau

import numpy as np
import pandas as pd
import re
from transliterate import translit, get_available_language_codes

# Библиотека для предобработки датасетов
import os, sys
module_path = os.path.abspath(os.path.join(os.pardir))
if module_path not in sys.path:
    sys.path.append(module_path)
from data_preprocessing import DataPreprocessor
dp = DataPreprocessor()

# Получение ClientID
def get_id(x):
    id = re.search('YA:.*', x).group(0)
    return re.sub('YA:', '', id).strip()


# Загружаем датасеты
df1 = pd.read_csv('../data/td_metrika.csv', ';')
df2 = pd.read_csv('../data/td_site.csv', ';')
df3 = pd.read_csv('../data/td_crm.csv', ';')

# Переименование и сортировка
df2.rename(columns={'st_id_ym': 'ym_clientid'}, inplace=True)
df2 = df2[df2['ym_clientid'] != 'undefined']

# Слияние датасетов
df4 = pd.merge(df1, df2, how='left')

# Переименование столбца и добавление префиксов
df3.rename(columns={'crm_client_id': 'ym_clientid'}, inplace=True)
df3.loc[df3['ym_clientid'] != 'undefined', 'ym_clientid'] = 'YA:' + df3['ym_clientid'].astype(str)
df4.loc[df4['ym_clientid'] != 'undefined', 'ym_clientid'] = 'YA:' + df4['ym_clientid'].astype(str)

# Слияние датасетов
df = pd.merge(df4, df3, how='outer')

# Корректируем ClientID
df['ym_clientid'] = df['ym_clientid'].apply(lambda x: get_id(x) if 'YA' in x else 'undefined', 1)

# Сохранение в файл
df.to_csv('../data/df_final.csv', index=False, encoding='utf-8', sep=';')
