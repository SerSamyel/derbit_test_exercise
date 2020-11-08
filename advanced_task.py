# coding: utf-8
import asyncio
import redis as red
from datetime import timedelta, datetime
import pandas
import requests as req
from typing import Tuple, List, Dict, Union
from json import loads

from get_nearest_exp import search_nearest_exp_instrument


class Config:

    @staticmethod
    def pandas_config():
        return pandas.set_option("display.max_rows", None,
                                 "display.max_columns", None,
                                 'display.width', 900)

    url = 'https://deribit.com/api/v2/public/get_tradingview_chart_data'
    requested_currency = ['BTC-PERPETUAL', 'ETH-PERPETUAL', search_nearest_exp_instrument()]
    resolution = 1  # data points frequency in minutes
    column_names = ['high', 'low', 'open', 'close', 'volume']
    since_midnight = True  # script work to start on this day
    update_minute = 0
    update_hour = 4
    redis_host = 'localhost'
    redis_port = 6379
    redis_decode_response = True
    window_size = 15  # minutes
    delay_seconds = 60  # time to repeat script
    timestamps_step = 60  # do not change in this version


async def get_data_from_url(instrument_name: str, start_time: datetime) -> Tuple[dict, int]:
    """Returns dictionary and status code for a given instrument name and start time,
       needs Config to work"""
    # mb try - expect
    resp = req.get(Config.url,
                   params={
                       'instrument_name': instrument_name,
                       'start_timestamp': int((start_time - timedelta(hours=Config.update_hour,
                                                                      minutes=Config.update_minute)).timestamp() * 1e3),
                       'end_timestamp': int(start_time.timestamp() * 1e3),
                       'resolution': Config.resolution
                   },
                   headers={'Content-Type': 'application/json'})
    data = resp.json().get('result')
    status = resp.status_code
    return data, status


def create_timestamp(start_time: datetime) -> List[str]:
    """Creates preformatted list with time data points filled in"""
    timestamps = [datetime.fromtimestamp(i).strftime('%Y-%m-%d %H:%M')
                  for i in range(int((start_time - timedelta(hours=Config.update_hour,
                                                             minutes=Config.update_minute)).timestamp()),
                                 int(start_time.timestamp()),
                                 Config.timestamps_step)
                  ]
    return timestamps


def generate_columns(currency: Union[str, list]) -> list:
    """Generates list of columns with currency names with suffixes depending on input type"""
    if isinstance(currency, str):
        return [f'{currency}_{column_name}' for column_name in Config.column_names]
    return [f'{cur}_{column_name}' for column_name in Config.column_names for cur in Config.requested_currency]


def create_dataframe(currency: str, timestamps: list) -> pandas.DataFrame:
    """Creates dataframe for currency and time stamps"""
    df = pandas.DataFrame(
        index=pandas.DatetimeIndex(timestamps,
                                   name="TIMESTAMP"),
        columns=generate_columns(currency=currency))
    return df


def update_dataframe(currency: str, dataframe: pandas.DataFrame, resource: dict) -> pandas.DataFrame:
    """Inserts data given in resource into dataframe"""
    # needs refactoring
    for i in range((Config.update_hour * 60 + Config.update_minute)):
        for key in resource.keys():
            if key == 'high':
                dataframe[f'{currency}_high'][i] = resource['high'][i]
            elif key == 'low':
                dataframe[f'{currency}_low'][i] = resource['low'][i]
            elif key == 'open':
                dataframe[f'{currency}_open'][i] = resource['open'][i]
            elif key == 'close':
                dataframe[f'{currency}_close'][i] = resource['close'][i]
            elif key == 'volume':
                dataframe[f'{currency}_volume'][i] = resource['volume'][i]
            else:
                continue
    return dataframe


async def publish_to_redis(df: pandas.DataFrame) -> None:
    """Commit dataframe data to redis"""
    r = red.Redis(host=Config.redis_host,
                  port=Config.redis_port,
                  )
    for col in df.columns:
        r.set(f'DATA_{col}', df[[col]].to_json())


def request_data_from_redis(name_data: str) -> Dict:
    """Query data from redis for name_data"""
    r = red.Redis(host=Config.redis_host,
                  port=Config.redis_port,
                  )

    date_string = r.get(f'DATA_{name_data}')
    date_dict = loads(date_string)
    return date_dict


def analyze_stats_data(data_dict: dict) -> Tuple[pandas.DataFrame, pandas.DataFrame, pandas.DataFrame]:
    """Analyze data_dict and calculate MA, HLR , return dataframes"""
    # not completed, need add 14 minute with 23-46 yesterday to 0-00 today
    # needs refactoring
    df = pandas.DataFrame(data_dict).rolling(Config.window_size)
    # MA
    df_mean = df.mean()
    df_mean.rename(columns={df_mean.keys()[0]: f"{df_mean.keys()[0]}_MA"}, inplace=True)
    # MAX
    df_max = df.max()
    df_max.rename(columns={df_max.keys()[0]: f"{df_max.keys()[0]}_MAX"}, inplace=True)
    # MIN
    df_min = df.min()
    df_min.rename(columns={df_min.keys()[0]: f"{df_min.keys()[0]}_MIN"}, inplace=True)
    return df_mean, df_max, df_min


# pandas configuration script
Config.pandas_config()


async def periodic():
    while True:
        start_time = datetime.now()
        if Config.since_midnight:
            Config.update_hours = start_time.hour
            Config.update_minute = start_time.minute
        timestamps = create_timestamp(start_time)
        main_df = pandas.DataFrame(index=pandas.DatetimeIndex(data=timestamps,
                                                              name='TIMESTAMP'),
                                   columns=generate_columns(Config.requested_currency))
        for currency_name in Config.requested_currency:
            data, status = await get_data_from_url(instrument_name=currency_name,
                                                   start_time=start_time)
            if status != 200:
                # logging - warning, status_code - error processing
                continue
            df = create_dataframe(currency=currency_name,
                                  timestamps=timestamps)

            df = update_dataframe(currency=currency_name,
                                  dataframe=df,
                                  resource=data)
            main_df.update(df)
        try:
            await publish_to_redis(main_df)
        except red.exceptions.ConnectionError as error:
            # logging - warning, maybe should save the data?
            continue

        # requested data from redis and analise data for MA and HLR
        # committed result to redis
        for currency in Config.requested_currency:
            for column in Config.column_names:
                data_name = request_data_from_redis(f'{currency}_{column}')
                analyze_result = analyze_stats_data(data_name)
                for data in analyze_result:
                    try:
                        await publish_to_redis(data)
                    except red.exceptions.ConnectionError as error:
                        # needs attention, logging and etc.
                        pass

        await asyncio.sleep(Config.delay_seconds)


if __name__ == '__main__':
    task = asyncio.get_event_loop().create_task(periodic())
    asyncio.get_event_loop().run_until_complete(task)
