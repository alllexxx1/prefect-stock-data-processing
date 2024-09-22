from datetime import timedelta
import json

from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.logging import get_run_logger
from prefect.tasks import task_input_hash
# from prefect.concurrency.sync import rate_limit

import httpx
import pandas as pd


STOCKS_API_KEY = Secret.load('stocks-api-key').get()
BOT_TOKEN = Secret.load('bot-token').get()
CHAT_ID = Secret.load('chat-id').get()


@task
def load_csv(file_path: str) -> pd.DataFrame:
    """
    Загружает csv файл в pandas DataFrame для извлечения
    тикера финансового инструмента (акции) для
    последующего обращения к соответствующему API.
    """

    logger = get_run_logger()
    logger.info('Данные из CSV-файла загружаются')
    df = pd.read_csv(file_path, delimiter=';')
    logger.info('Данные из CSV-файла загружены')
    return df


@task(
    retries=3,
    retry_delay_seconds=5,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1)
)
def fetch_stock_data(url: str, symbol: str) -> dict:
    """
    Обращается к указанному API для получения данных о
    конкретном финансовом инструменте. Тикер инструмента
    указывается через аргумент "symbol".
    """

    logger = get_run_logger()

    try:
        response = httpx.get(url)
        response.raise_for_status()
        stock_data = response.json()

        logger.info(f'Данные получены для тикера {symbol}')
        return stock_data

    except Exception as e:
        logger.error(f'Вызов API для тикера {symbol} провалился: {e}')



@task(log_prints=True)
def process_stock_data(stock_data: dict) -> list[dict]:
    """
    Обрабатывает данные финансового инструмента, полученные
    в результате обращения к API и создает структуру для дальнейшего
    сохранения необходимой информации в файл формата json.
    """

    logger = get_run_logger()

    time_series = stock_data.get('Time Series (Daily)', {})
    processed_data = [
        {
            "date": date,
            "open": values["1. open"],
            "high": values["2. high"],
            "low": values["3. low"],
            "close": values["4. close"],
            "volume": values["5. volume"]
        }
        for date, values in time_series.items()
    ]
    logger.info(f'Данные обработаны')
    return processed_data


@task
def save_as_json(processed_data: list[dict], symbol: str) -> None:
    """
    Сохраняет полученные и обработанные данные в файл json
    в локальном хранилище.
    """

    logger = get_run_logger()
    file_path = f"output_stock_data/{symbol}_stock_data.json"
    with open(file_path, 'w') as f:
        json.dump(processed_data, f)

    logger.info(f'Данные для тикета {symbol} сохранены')


@task
def send_telegram_notification(message: str) -> None:
    """
    Отправляет сообщение в указанный телеграм-чат в случае
    успеха/провала получения и обработки данных.

    Использует телеграм API. Для успешной отправки нужен токен
    телеграм бота и id чата в который отправляется уведомление.
    """

    logger = get_run_logger()
    bot_token = BOT_TOKEN
    chat_id = CHAT_ID

    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    params = {
        'chat_id': chat_id,
        'text': message,
    }

    response = httpx.post(url, params=params)
    if response.status_code == 200:
        logger.info('Сообщение об успешной обработке данных отправлено в Телеграм чат')
    else:
        logger.error(f'Ошибка при отправке сообщения в Телеграм чат: {response.status_code}')


@flow
def stock_data_processing_flow(input_file: str, stocks_api_key: str):
    """
    Главный flow всего пайплайна, комбинирующий операции
    извлечения данных из csv файла, обращения к API, обработки
    полученных данных, сохранения данных и отправки уведомления в
    телеграм-чат.
    """

    logger = get_run_logger()

    df = load_csv(input_file)
    for symbol in df['symbol']:

        stock_data = fetch_stock_data(
            f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={stocks_api_key}',
            symbol
        )
        
        processed_stock_data = process_stock_data(stock_data)
        logger.info(processed_stock_data)
        save_as_json(processed_stock_data, symbol)

    send_telegram_notification('Данные успешно обработаны')


if __name__ == '__main__':
    stock_data_processing_flow(
        input_file='stock_indicators.csv',
        stocks_api_key=STOCKS_API_KEY
    )
