from binance.client import Client
import pandas as pd
from utils import configure_logging
from multiprocessing import Process, freeze_support, Pool, cpu_count
import os

try:
    from credentials import API_KEY, API_SECRET
except ImportError:
    API_KEY = API_SECRET = None
    exit("CAN'T RUN SCRIPT WITHOUT BINANCE API KEY/SECRET")

log = configure_logging()


class FuturesDataPuller(Process):

    SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'LTCUSDT', 'LINKUSDT']
    # 'BTCUSDT', 'XRPUSDT', 'SXPUSDT', 'ADAUSDT', 'EOSUSDT', 'DOTUSDT', 'VETUSDT', 'ETHUSDT', 'LTCUSDT', 'LINKUSDT'
    KLINE_INTERVALS = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']

    def __init__(self, client, symbol, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client
        self.symbol = symbol

    def run(self):
        klines = self.get_klines(
            interval='1m',
            start_date="15 Feb 2021 00:00:00",
            end_date="19 Feb 2021 12:00:00",
        )
        funding_rates = self.get_funding_rate(klines=klines)
        df = self.reformat_data(klines=klines, funding_rates=funding_rates)

        self.to_csv(df=df)

    def get_klines(self, interval, start_date, end_date):
        """

        :param interval: str, one of the supported intervals from KLINES_INTERVALS list
        :param start_date: str, format 'DD MMM YYYY HH:mm:ss'
        :param end_date: str
        :return: list of lists with klines[Open_time: int,
                                           Open: Decimal,
                                           High: Decimal,
                                           Low: Decimal,
                                           Close: Decimal,
                                           Volume: Decimal,
                                           Close_time: int,
                                           Quote asset volume: Decimal,
                                           Number of trades: int,
                                           Taker buy base asset volume: Decimal,
                                           Taker buy quote asset volume: Decimal,
                                           Ignore: Decimal]
        """
        try:
            data = self.client.get_historical_klines(
                symbol=self.symbol,
                interval=interval,
                start_str=start_date,
                end_str=end_date,
            )
        except Exception as exc:
            log.exception(exc)
            return {'msg': exc}
        return data

    def get_funding_rate(self, klines: list):
        """
        Uses first and last kline time to get funding rates for that period

        :param klines: trade klines
        :return: list of dicts(symbol=(str), fundingTime=(int), fundingRate=(Decimal))
        """
        start, bypass_limit, end = klines[0][0], klines[int(len(klines)/2)][0], klines[len(klines) - 1][0]
        try:
            data = self.client.futures_funding_rate(
                symbol=self.symbol,
                startTime=start,
                endTime=bypass_limit,
            )
            data_2 = self.client.futures_funding_rate(
                symbol=self.symbol,
                startTime=bypass_limit,
                endTime=end,
            )
        except Exception as exc:
            log.exception(exc)
            return {'msg': exc}
        for instance in data_2:
            data.append(instance)
        return data

    def to_csv(self, df):
        """

        :param df: pd.DataFrame obj.
        :return: .csv file with data
        """
        file_directory = 'data'
        file_full_path = os.path.join(file_directory, f'{self.symbol}.csv')
        if not os.path.exists(file_directory):
            os.makedirs(name=file_directory)
        df.to_csv(path_or_buf=file_full_path, sep=',')

    @staticmethod
    def reformat_data(klines, funding_rates):
        """

        :return: pd.DataFrame obj. with required_data
        """
        df = pd.DataFrame.from_records(klines)
        df = df.drop(range(5, 12), axis=1)
        col_names = ['time', 'open', 'high', 'low', 'close']
        df.columns = col_names
        for col in col_names:
            df[col] = df[col].astype(float)
        df['date'] = pd.to_datetime(df['time'] * 1000000, format='%Y-%m-%d %H:%M:%S')
        df['date'] = df['date'].dt.floor('s')
        df['close_diff'] = (df['close'] - df['open']) / df['open']
        df['fundingRate'] = None

        df_fund = pd.DataFrame.from_records(funding_rates)
        df_fund = df_fund.drop(columns='symbol')
        for column in df_fund.columns:
            df_fund[column] = df_fund[column].astype(float)
        df_fund['date'] = pd.to_datetime(df_fund['fundingTime'] * 1000000, format='%Y-%m-%d %H:%M:%S')
        df_fund['date'] = df_fund['date'].dt.floor('s')
        for ind, date in enumerate(df['date']):
            for ind_2, date_2 in enumerate(df_fund['date']):
                if date == date_2:
                    df.iat[ind, 7] = df_fund.iat[ind_2, 1]

        return df


def main():
    client = Client(API_KEY, API_SECRET)
    pool = Pool(processes=cpu_count())
    pullers = []
    for symbol in FuturesDataPuller.SYMBOLS:
        puller = FuturesDataPuller(client=client, symbol=symbol)
        pullers.append(puller)
    for puller in pullers:
        pool.apply_async(puller.run())
    while True:
        if not any(puller.is_alive() for puller in pullers):
            break
    pool.close()
    pool.join()


if __name__ == '__main__':
    freeze_support()
    main()
