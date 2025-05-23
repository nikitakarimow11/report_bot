# 📊 Report Bot

Автоматизация расчёта прибыли агрегатора цифровых сервисов.

Скрипт на Python, который подключается к Google Sheets, анализирует транзакции и генерирует отчёт по чистой прибыли на данных, заполненных менеджером.

---

## 🚀 Возможности

- Расчёт чистой прибыли с учётом комиссий, валют и источников
- Сводка по дням, неделям и месяцам
- Автоматическое создание новых листов в Google Sheets
- Поддержка TRY → RUB и USD → RUB с учётом курса
- Уведомления/интеграция с Telegram

---

## 🛠 Используемые технологии

- Python (pandas, datetime, requests)
- Google Sheets API (gspread)
- Telegram Bot API
- .env-файл и `.gitignore` для безопасности

---

## 📦 Структура
📁 report_bot
├── g4u_report.py # Основной скрипт
├── .gitignore # API гугл листов
└── README.md # Описание проекта

## ▶️ Как запустить бота

1. Установите зависимости:
2. Вставьте в код ваш токен тг бота
3. Убедитесь, что в проекте присутствует файл g4u_api.json с доступом к Google Sheets API
4. Запустите скрипт

   ## 📸 Скриншоты

### 📊 Пример отчёта в Google Sheets
![Google Sheets](screenshots/google_sheet_report.png)

### 📲 Уведомление от Telegram-бота
![Telegram Bot](screenshots/telegram_notify.png)

