# INHUB - Analytics

[![v1.0.0](https://img.shields.io/github/manifest-json/v/chegevarae/inhub?filename=extension%2Fmanifest.json)](https://img.shields.io/github/manifest-json/v/chegevarae/inhub?filename=extension%2Fmanifest.json) [![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)  
Программное обеспечение для обработки данных с целью их последующего анализа и использования в моделях машинного обучения для предсказания вероятности покупки ПО.  

**Поддерживает следующие функции:**  
- Обработка csv файлов для витрин данных, источники: калькулятор, метрика, crm  
- Прямое подключение к MySQL + API метрики и CRM через коннекторы <span style="color:blue">🛠</span>  
- Получение информации о пользователе из Метрики по его ID <span style="color:blue">🛠</span>  
- Предсказание вероятности покупки ПО при помощи модели бинарной классификации <span style="color:blue">🛠</span>  

**Примеры работы:**  
- Обработка csv файлов: [Витрина данных →](https://public.tableau.com/profile/chegevara#!/vizhome/Analytics_16164172063330/Industies-Dashboard)  

[![mockup](images/mockup.png)](images/mockup.png)  

## Обязательные требования

- Python 3.7+  
- Библиотеки Python: re, nampy, pandas, transliterate  
- Полный список зависимостей в файле requirements.txt  
- Наличие данных для обработки  
- Api Метрики и Api Битрикс24  

## Аналитика Tableau Desktop

1. Скачиваем и устанавливаем [![Tableau Desktop](https://www.tableau.com/products/desktop/download)](https://www.tableau.com/products/desktop/download)  
2. Получаем сырые данные из калькулятора, метрики и crm - перечень файлов см. в папке data  
3. Обрабатываем .csv файлы etl-скриптами из папки scr:  
    - etl_crm.py  
    - etl_metrika.py  
    - etl_site.py  
4. Скачиваем актуальный проект [![Analytics](https://public.tableau.com/profile/chegevara#!/vizhome/Analytics_16164172063330/Industies-Dashboard)](https://public.tableau.com/profile/chegevara#!/vizhome/Analytics_16164172063330/Industies-Dashboard)  
5. Открываем Analytics в Tableau Desktop и на витрину подаем .csv файлы:
    - td_crm.py  
    - td_metrika.py  
    - td_site.py  
