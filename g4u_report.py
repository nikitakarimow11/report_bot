import pandas as pd
import requests
import gspread
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# === НАСТРОЙКИ ===
SOURCE_SPREADSHEET = 'отчет_заказы'
SOURCE_SHEET = 'Выручка'
TARGET_SPREADSHEET = 'profit'
TARGET_SHEET = 'Лист1'
SERVICE_FILE = 'g4u_api.json'

CURRENCY_MARKUP = 1.05  # наценка 5%

# === Авторизация Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_FILE, scope)
client = gspread.authorize(creds)

# === Загрузка данных
source_ws = client.open(SOURCE_SPREADSHEET).worksheet(SOURCE_SHEET)
data = source_ws.get_all_records()
df = pd.DataFrame(data)

# === Приведение даты
df['Дата'] = pd.to_datetime(df['Дата'], format='%d.%m.%y', errors='coerce')

# === Кэш курсов
exchange_cache = {}

# === Получение курса ЦБ РФ с откатом на 7 дней
def get_cbr_rate(currency_code: str, date: datetime) -> float:
    for i in range(7):
        check_date = date - timedelta(days=i)
        date_str = check_date.strftime("%d/%m/%Y")
        if (currency_code, date_str) in exchange_cache:
            return exchange_cache[(currency_code, date_str)]
        
        url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={date_str}"
        try:
            response = requests.get(url, timeout=10)
            root = ET.fromstring(response.content)
            for valute in root.findall('Valute'):
                char_code = valute.find('CharCode').text
                if char_code == currency_code:
                    nominal = int(valute.find('Nominal').text)
                    value = float(valute.find('Value').text.replace(',', '.'))
                    rate = (value / nominal) * CURRENCY_MARKUP
                    exchange_cache[(currency_code, date_str)] = rate
                    return rate
        except:
            continue
    return 0

# === Обработка себестоимости
def parse_cost_with_date(row):
    val = str(row.get("Себестоимость, TL", "")).strip().lower().replace(",", ".")
    date = row["Дата"]
    if pd.isnull(date) or not val or val == "0":
        return 0
    try:
        if "usdt" in val or "$" in val:
            amount = float(val.replace("usdt", "").replace("$", "").strip())
            rate = get_cbr_rate("USD", date)
        else:
            amount = float(val)
            rate = get_cbr_rate("TRY", date)
        return round(amount * rate, 2)
    except:
        return 0

# === Очистка выручки
def clean_revenue(val):
    try:
        if isinstance(val, str):
            return float(val.replace("Авито", "").strip())
        return float(val)
    except:
        return 0

# === Расчёты
df["Себестоимость, руб"] = df.apply(parse_cost_with_date, axis=1)
df["Выручка, руб"] = df["Выручка, руб"].apply(clean_revenue)
df["Чистая прибыль"] = df["Выручка, руб"] - df["Себестоимость, руб"]

# === Форматирование
df["Дата"] = df["Дата"].dt.strftime("%Y-%m-%d")
df = df.fillna("")

# === Дублируем датафрейм чтобы далее считать данные в разрезе дня/месяца
df_2 = (
    df.dropna(subset=["Площадка"])
      .query("Площадка != ''")
      .groupby(["Дата", "Площадка"])["Чистая прибыль"]
      .sum()
      .reset_index()
      .sort_values(by=["Дата", "Площадка"])
)

df_2["Чистая прибыль"] = df_2["Чистая прибыль"].round(2)

# === Запись df_2 на отдельный лист
sheet_name = "Прибыль по площадкам"

# Удалим лист, если он уже есть
try:
    client.open(TARGET_SPREADSHEET).del_worksheet(
        client.open(TARGET_SPREADSHEET).worksheet(sheet_name)
    )
except:
    pass

# Создадим новый лист и запишем туда df_2
ws_2 = client.open(TARGET_SPREADSHEET).add_worksheet(title=sheet_name, rows=100, cols=10)
ws_2.update([df_2.columns.values.tolist()] + df_2.values.tolist())

# === Запись в Google Sheets
target_ws = client.open(TARGET_SPREADSHEET).worksheet(TARGET_SHEET)
target_ws.clear()
target_ws.update([df.columns.values.tolist()] + df.values.tolist())

# === Отправка прибыли за последний день в Telegram

# === Обработка прибыли за последний день
df_2["Дата"] = pd.to_datetime(df_2["Дата"])
last_day = df_2["Дата"].max()
df_last = df_2[df_2["Дата"] == last_day]

# === Формируем строку отчёта
lines = [f"<b>📊 Отчёт за {last_day.strftime('%d.%m.%Y')}:</b>"]
for _, row in df_last.iterrows():
    platform = row["Площадка"]
    value = row["Чистая прибыль"]
    label = "Выручка Авито" if "avito" in platform.lower() else "Прибыль Telegram"
    lines.append(f"— {label}: {value:,.2f} ₽")

msg = "\n".join(lines)

# === Настройки Telegram
TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"  # токен бота
TELEGRAM_CHAT_ID = "1355514400"  # 🔁 мой айди
SEND_TELEGRAM = True  #  Управление отправкой

# === Функция отправки в Telegram
def send_profit_to_telegram(bot_token, chat_id, message):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        r = requests.post(url, data=payload)
        r.raise_for_status()
        print("✅ Сообщение отправлено в Telegram")
    except Exception as e:
        print(f"❌ Ошибка отправки в Telegram: {e}")

# === Отправка сообщения
if SEND_TELEGRAM:
    send_profit_to_telegram(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, msg)
else:
    print("✉️ Отправка в Telegram отключена (SEND_TELEGRAM = False)")