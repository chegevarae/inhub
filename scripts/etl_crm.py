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

# Функция получения ClientID
def get_id(x):
    id = re.search('YA:.*', x).group(0)
    return re.sub('YA:', '', id).strip()

# Функция удаления столбцов
def drop_columns(df, columns):
    for column in columns:
        df = df.drop(column, axis=1)  
    return df

# Функция транслитерации
def get_translite(columns):
    lst = []
    prefix = 'CRM_'
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


# Загружаем датасет
df = pd.read_csv('../data/LEAD.csv', encoding='cp1251', sep=';')

# Удаление ненужных данных
columns = ['Дата рождения', 'Рабочий телефон', 'Мобильный телефон', 'Номер факса', 'Домашний телефон', 'Номер пейджера', 
           'Телефон для рассылок', 'Другой телефон', 'Личная страница', 'Страница Facebook', 'Страница ВКонтакте', 
           'Страница LiveJournal', 'Микроблог Twitter', 'Другой сайт', 'Частный e-mail', 'E-mail для рассылок', 
           'Другой e-mail', 'Контакт Facebook', 'Контакт Telegram', 'Контакт ВКонтакте', 'Контакт Skype', 'Контакт Viber', 
           'Комментарии Instagram', 'Контакт Битрикс24.Network', 'Онлайн-чат', 'Контакт Открытая линия', 'Контакт ICQ', 
           'Контакт MSN/Live!', 'Контакт Jabber', 'Другой контакт', 'Ответственный', 'Дополнительно о статусе', 'Кем создан', 
           'Дата изменения', 'Кем изменен', 'Улица, номер дома', 'Квартира, офис, комната, этаж', 'Район', 'Регион', 
           'Почтовый индекс', 'Страна', 'Комментарий', 'Создан CRM-формой', 'Клиент', 'Путь клиента', 'Компоненты', 
           'Причина отказа сделки', 'Причина отказа сделки (список)', 'Проект', 'Конфигурация 1С', 'Наличие 1С', 
           'Причина отказа лида (список)', 'Партнерство', 'Подписан на рассылку', 'UF_CRM_TEXTAREA', 'UF_CRM_TRANID', 
           'UF_CRM_FORMNAME', 'UF_CRM_COOKIES', 'UF_CRM_VASHESOOBSCHE', 'Отправка списка компонентов']
df = drop_columns(df, columns)

# Переименование столбцов
df.columns = get_translite(df.columns)

# Корректировка значений
df.loc[df['crm_obraschenie'].isnull(), 'crm_obraschenie'] = 'undefined'
df.loc[df['crm_imja'].isnull(), 'crm_imja'] = 'undefined'
df.loc[df['crm_otchestvo'].isnull(), 'crm_otchestvo'] = 'undefined'
df.loc[df['crm_familija'].isnull(), 'crm_familija'] = 'undefined'
df.loc[df['crm_istochnik'].isnull(), 'crm_istochnik'] = 'undefined'
df.loc[df['crm_korporativnyj_sajt'].isnull(), 'crm_korporativnyj_sajt'] = 'undefined'
df.loc[df['crm_rabochij_e_mail'].isnull(), 'crm_rabochij_e_mail'] = 'undefined'
df.loc[df['crm_dopolnitelno_ob_istochnike'].isnull(), 'crm_dopolnitelno_ob_istochnike'] = 'undefined' # Для регулярки
df.loc[df['crm_nazvanie_kompanii'].isnull(), 'crm_nazvanie_kompanii'] = 'undefined'
df.loc[df['crm_dolzhnost'].isnull(), 'crm_dolzhnost'] = 'undefined'
df.loc[df['crm_adres'].isnull(), 'crm_adres'] = 'undefined'
df.loc[df['crm_naselennyj_punkt'].isnull(), 'crm_naselennyj_punkt'] = 'undefined'
df.loc[df['crm_utm_source'].isnull(), 'crm_utm_source'] = 'undefined'
df.loc[df['crm_utm_medium'].isnull(), 'crm_utm_medium'] = 'undefined'
df.loc[df['crm_utm_campaign'].isnull(), 'crm_utm_campaign'] = 'undefined'
df.loc[df['crm_utm_content'].isnull(), 'crm_utm_content'] = 'undefined'
df.loc[df['crm_utm_term'].isnull(), 'crm_utm_term'] = 'undefined'
df.loc[df['crm_klassifikator_otrasli'].isnull(), 'crm_klassifikator_otrasli'] = 'undefined'
df.loc[df['crm_emkost_litsenzii'].isnull(), 'crm_emkost_litsenzii'] = df['crm_emkost_litsenzii'].median()
df.loc[df['crm_otrasl'].isnull(), 'crm_otrasl'] = 'undefined'
df.loc[df['crm_otrasl_spisok'].isnull(), 'crm_otrasl_spisok'] = 'undefined'
df.loc[df['crm_stranitsa_v_internete'].isnull(), 'crm_stranitsa_v_internete'] = 'undefined'
df.loc[df['crm_istochnik_trafika'].isnull(), 'crm_istochnik_trafika'] = 'undefined'

# Получаем ClientID
df['client_id'] = df['crm_dopolnitelno_ob_istochnike'].apply(lambda x: get_id(x) if 'YA' in x else 'undefined', 1)
df.loc[(df['client_id'].isnull()) | (df['client_id'] == ''), 'client_id'] = 'undefined'

# Новый порядок столбцов
new_order = [30, 0, 7, 1, 2, 3, 4, 5, 6, 13, 12, 8, 9, 10, 11, 14, 15, 28, 29, 25, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26, 27]
df = df[df.columns[new_order]]

# Приведение типов
df = astype_col(df, ['crm_data_sozdanija'], coltype='datetime64')
df = astype_col(df, ['crm_emkost_litsenzii'], coltype='uint8')

# Удалим дубликаты по crm_id
df = df.drop_duplicates(subset='crm_id', keep="first")

# Скорректируем дату для более удобной группировки данных
df['crm_data_sozdanija'] = pd.to_datetime(df['crm_data_sozdanija']).dt.strftime('%Y-%m-%d')

# Сохраняем в файл
df.to_csv('../data/td_crm.csv', index=False, encoding='utf-8', sep=';')