'''

# Library for preprocessing datasets
# Библиотека для предобработки датасетов

import os, sys
module_path = os.path.abspath(os.path.join(os.pardir))
if module_path not in sys.path:
    sys.path.append(module_path)
from data_preprocessing import DataPreprocessor
dp = DataPreprocessor()

df = dp.myfunc(df)

'''

import numpy as np
import pandas as pd
import re
from transliterate import translit, get_available_language_codes


class DataPreprocessor:

    def group_features(self, df, TARGET='target'):
        '''Группировка признаков по типам

        TARGET          -- целевая переменная
        BASE_FEATURE    -- базовые фичи
        NUM_FEATURE     -- вещественные фичи
        CAT_FEATURE     -- категориальные фичи

        >>> group_features(df, TARGET)
        '''
        try: BASE_FEATURE = df.columns.drop(TARGET).tolist()
        except: BASE_FEATURE = df.columns.tolist()
        CAT_FEATURE = df[BASE_FEATURE].select_dtypes(include='object').columns.tolist()
        NUM_FEATURE = df[BASE_FEATURE].columns.drop(CAT_FEATURE).tolist()
        return TARGET, BASE_FEATURE, NUM_FEATURE, CAT_FEATURE


    def astype_col(self, df, colgroup, coltype):
        '''Приведение типов
        
        colgroup    -- список фич
        coltype     -- тип данных

        >>> astype_col(df, ['colgroup'], 'coltype')
        '''
        for colname in colgroup:
            df[colname] = df[colname].astype(coltype)
        return df


    def drop_col(self, df, colgroup):
        '''Удаление столбцов
        
        >>> drop_col(df, colgroup)
        '''
        for column in colgroup:
            df = df.drop(column, axis=1)  
        return df


    def get_translite(self, colgroup, prefix=None):
        '''Транслитерация имен столбцов

        colgroup    -- список фич
        prefix_     -- префикс для группировки фич

        >>> get_translite(colgroup, 'prefix_')
        '''
        lst = []
        for column in colgroup:
            r = translit(prefix + column, 'ru', reversed=True).lower()
            r = re.sub("\'|\(|\)|:", "", r)
            r = re.sub("\ |-|‑", "_", r)
            lst.append(r)
        return lst


    def get_idx_col(self, colgroup):
        '''Получение имен столбцов с индексом
        
        >>> get_idx_col(colgroup)
        '''
        lst = []
        for index, value in enumerate(colgroup):
            print(index, value)
            lst.append(index)
        print(lst)
