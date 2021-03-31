'''
Вспомогательные функции для обработки данных.
'''

import numpy as np
import pandas as pd
import re
from transliterate import translit, get_available_language_codes

# Группировка признаков по типам
def group_features(df, TARGET='target'):
    try: BASE_FEATURE = df.columns.drop(TARGET).tolist()
    except: BASE_FEATURE = df.columns.tolist()
    CAT_FEATURE = df[BASE_FEATURE].select_dtypes(include='object').columns.tolist()
    NUM_FEATURE = df[BASE_FEATURE].columns.drop(CAT_FEATURE).tolist()
    return TARGET, BASE_FEATURE, NUM_FEATURE, CAT_FEATURE

# Приведение типов
def astype_col(df, colgroup, coltype):
    for colname in colgroup:
        df[colname] = df[colname].astype(coltype)
    return df

# Удаление столбцов
def drop_col(df, colgroup):
    for column in colgroup:
        df = df.drop(column, axis=1)  
    return df

# Транслитерация имен столбцов
def get_translite(colgroup, prefix=None):
    lst = []
    for column in colgroup:
        r = translit(prefix + column, 'ru', reversed=True).lower()
        r = re.sub("\'|\(|\)|:", "", r)
        r = re.sub("\ |-|‑", "_", r)
        lst.append(r)
    return lst

# Получение имен столбцов с индексом
def get_idx_col(colgroup):
    lst = []
    for index, value in enumerate(colgroup):
        print(index, value)
        lst.append(index)
    print(lst)

# Получение ClientID
def get_id(x):
    id = re.search('YA:.*', x).group(0)
    return re.sub('YA:', '', id).strip()
