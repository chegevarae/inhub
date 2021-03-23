# INHUB - Analytics
[![v1.0.0](https://img.shields.io/github/manifest-json/v/chegevarae/inhub?filename=extension%2Fmanifest.json)](https://img.shields.io/github/manifest-json/v/chegevarae/inhub?filename=extension%2Fmanifest.json) [![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)  
Программное обепечение для обработки данных с целью их последующего анализа и использования в моделях машинного обучения для предсказания вероятности покупки ПО.  

**Поддерживает следующие функции:**  
- Обработка csv файлов для витрин данных, источники: калькулятор, метрика, crm  
- Прямое подключение к MySQL + API метрики и CRM через коннекторы <span style="color:blue">🛠</span>  
- Получение информации о пользователе из Метрики по его ID <span style="color:blue">🛠</span>  
- Предсказание вероятности покупки ПО при помощи модели бинарной классификации <span style="color:blue">🛠</span>  

[Витрина данных](https://public.tableau.com/profile/chegevara#!/vizhome/Analytics_16164172063330/Dashboard-Industies "Готовые дашборды")  

[![mockup](images/mockup.png)](images/mockup.png)  

## Обязательные требования
- Python 3.7+  
- Библиотеки Python: re, nampy, pandas, transliterate  
- Наличие данных для обработки
- Api Метрики и Api Битрикс24
