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
    prefix = 'YM_'
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


# Загружаем датасеты без 1-й строки и делаем переиндексацию с удалением столбца index
df1 = pd.read_csv('../data/hubex.ru-ClientID-Sources.csv')[1:].reset_index(drop=True) # Источники
df2 = pd.read_csv('../data/hubex.ru-ClientID-Behavior.csv')[1:].reset_index(drop=True) # Поведение
# Удаляем дубликаты по ClientID
df1 = df1.drop_duplicates(subset='ClientID', keep="first")
df2 = df2.drop_duplicates(subset='ClientID', keep="first")

# Производим слияние датасетов
df = pd.merge(df1, df2, how='outer')

# Удаление ненужных данных
columns = ['Посетители']
df = drop_columns(df, columns)

# Сегментация пользователей
df.loc[df['Дата визита'].isnull(), 'Дата визита'] = 'undefined'

# Переименование столбцов
df.columns = get_translite(df.columns)

# Корректировка значений
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Не определено', 'ym_pervyj_istochnik_trafika'] = 'undefined'
# ------ Для склейки заменяем пропуски на np.nan
df.loc[df['ym_pervyj_perehod_s_sajtov'] == 'Не определено', 'ym_pervyj_perehod_s_sajtov'] = np.nan
df.loc[df['ym_pervaja_poiskovaja_sistema'] == 'Не определено', 'ym_pervaja_poiskovaja_sistema'] = np.nan
df.loc[df['ym_utm_source'] == 'Не определено', 'ym_utm_source'] = np.nan
df.loc[df['ym_utm_content'] == 'Не определено', 'ym_utm_content'] = np.nan
# ------
df.loc[df['ym_gorod'].isnull(), 'ym_gorod'] = 'undefined'
df.loc[df['ym_gorod'] == 'Не определено', 'ym_gorod'] = 'undefined'
df.loc[df['ym_otkazy'].isnull(), 'ym_otkazy'] = df['ym_otkazy'].median()
df.loc[df['ym_glubina_prosmotra'].isnull(), 'ym_glubina_prosmotra'] = df['ym_glubina_prosmotra'].median()
df.loc[df['ym_vremja_na_sajte'].isnull(), 'ym_vremja_na_sajte'] = 'undefined'
df.loc[df['ym_novye_posetiteli'].isnull(), 'ym_novye_posetiteli'] = 'undefined'
df.loc[df['ym_dnej_mezhdu_vizitami'].isnull(), 'ym_dnej_mezhdu_vizitami'] = df['ym_dnej_mezhdu_vizitami'].median()
df.loc[df['ym_vernuvshiesja_1_den'].isnull(), 'ym_vernuvshiesja_1_den'] = df['ym_vernuvshiesja_1_den'].median()
df.loc[df['ym_vernuvshiesja_2_7_dnej'].isnull(), 'ym_vernuvshiesja_2_7_dnej'] = df['ym_vernuvshiesja_2_7_dnej'].median()
df.loc[df['ym_data_pervogo_vizita'].isnull(), 'ym_data_pervogo_vizita'] = 'undefined'
df.loc[df['ym_dostignutaja_tsel'].isnull(), 'ym_dostignutaja_tsel'] = 'undefined'
df.loc[df['ym_dostignutaja_tsel'] == 'Не определено', 'ym_dostignutaja_tsel'] = 'undefined'
df.loc[df['ym_put_polnyj_stranitsy_vhoda'].isnull(), 'ym_put_polnyj_stranitsy_vhoda'] = 'undefined'
df.loc[df['ym_put_polnyj_stranitsy_vyhoda'].isnull(), 'ym_put_polnyj_stranitsy_vyhoda'] = 'undefined'
df.loc[df['ym_mobilnost'].isnull(), 'ym_mobilnost'] = 'undefined' # Если не определено
df.loc[df['ym_mobilnost'] == 1.0, 'ym_mobilnost'] = 1
df.loc[df['ym_mobilnost'] == 0.0, 'ym_mobilnost'] = 0

# Приведение типов
df = astype_col(df, ['ym_vizity'], coltype='uint8')

# Добавляем стоимость клика по каждому каналу
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Прямые заходы', 'ym_cost'] = 0
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы по рекламе', 'ym_cost'] = 15
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы из поисковых систем', 'ym_cost'] = 48.5
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Внутренние переходы', 'ym_cost'] = 0
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы по ссылкам на сайтах', 'ym_cost'] = 0
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы из социальных сетей', 'ym_cost'] = 0
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы с почтовых рассылок', 'ym_cost'] = 2
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы из рекомендательных систем', 'ym_cost'] = 0
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы из мессенджеров', 'ym_cost'] = 0
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы с сохранённых страниц', 'ym_cost'] = 0
df.loc[df['ym_pervyj_istochnik_trafika'].isnull(), 'ym_pervyj_istochnik_trafika'] = 'undefined'
df.loc[df['ym_pervyj_istochnik_trafika'] == 'undefined', 'ym_cost'] = 0

# Переименовываем категории каналов
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Прямые заходы', 'ym_pervyj_istochnik_trafika'] = 'Browser-Traffic'
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы по рекламе', 'ym_pervyj_istochnik_trafika'] = 'Ad-Traffic'
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы из поисковых систем', 'ym_pervyj_istochnik_trafika'] = 'Search'
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Внутренние переходы', 'ym_pervyj_istochnik_trafika'] = 'Site-Traffic'
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы по ссылкам на сайтах', 'ym_pervyj_istochnik_trafika'] = 'Sites'
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы из социальных сетей', 'ym_pervyj_istochnik_trafika'] = 'Social'
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы с почтовых рассылок', 'ym_pervyj_istochnik_trafika'] = 'Email'
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы из рекомендательных систем', 'ym_pervyj_istochnik_trafika'] = 'RecSys'
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы из мессенджеров', 'ym_pervyj_istochnik_trafika'] = 'Messenger'
df.loc[df['ym_pervyj_istochnik_trafika'] == 'Переходы с сохранённых страниц', 'ym_pervyj_istochnik_trafika'] = 'SavedPages'

# Слияние столбцов
df['ym_source'] = df[df.columns[2:6]].apply(lambda x: ' > '.join(x.dropna().astype(str)), axis=1)

# Новый порядок столбцов
new_order = [0, 1, 23, 2, 3, 4, 5, 6, 22, 16, 17, 7, 8, 9, 10, 11, 12, 13, 14, 15, 18, 19, 20, 21]
df = df[df.columns[new_order]]

# Корректировка значений
df['ym_source'] = df['ym_source'].str.lower() # Переводим все нижний регистр
df.loc[(df['ym_source'] == 'яндекс') | (df['ym_source'] == 'yandex.ru'), 'ym_source'] = 'yandex'
df.loc[(df['ym_source'] == '') | (df['ym_source'].isnull()), 'ym_source'] = 'undefined' # Убираем Null и пустоты
df = df[~df['ym_source'].str.contains('([0-9]{1,3}[\.]){3}[0-9]{1,3}')] # Убираем ip-адреса
df.loc[df['ym_source'] == 'мобильные%сотрудники', 'ym_source'] = 'undefined'
df.loc[df['ym_source'] == 'старт за 2 дня', 'ym_source'] = 'undefined'
df.loc[df['ym_source'] == 'яндекс > serp', 'ym_source'] = 'yandex'
# -------
df.loc[df['ym_pervyj_perehod_s_sajtov'].isnull(), 'ym_pervyj_perehod_s_sajtov'] = 'undefined'
df.loc[df['ym_pervaja_poiskovaja_sistema'].isnull(), 'ym_pervaja_poiskovaja_sistema'] = 'undefined'
df.loc[df['ym_utm_source'].isnull(), 'ym_utm_source'] = 'undefined'
df.loc[df['ym_utm_content'].isnull(), 'ym_utm_content'] = 'undefined'

# Сохраняем в файл
df.to_csv('../data/td_metrika.csv', index=False, encoding='utf-8', sep=';')
