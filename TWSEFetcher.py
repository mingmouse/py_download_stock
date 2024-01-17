import requests
import os
import csv
import datetime
from collections import namedtuple
import urllib.parse
import time
import requests
import random
TWSE_BASE_URL = 'https://www.twse.com.tw'
StockData = namedtuple('StockData', ['date', 'volume', 'turnover', 'opening_price', 'highest_price', 'lowest_price', 'closing_price', 'change', 'transaction'])
YieldData = namedtuple('YieldData', ['stock_id', 'dividend_yield', 'dividend_year', 'pe_ratio', 'pb_ratio', 'financial_year'])


class TWSEFetcher:
    def __init__(self):
        self.session = requests.Session()
    
    def fetch_data(self, url, params, max_retries=1, backoff_factor=0.3):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        for attempt in range(max_retries):
            try:
                #time.sleep(backoff_factor) 
                response = self.session.get(url, params=params, headers=headers)
                response.raise_for_status()
                 # 在请求之间增加固定间隔
                return response.json()
            except Exception as e:
                return None
    def save_as_csv(self, data, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(data[0]._fields)
            for row in data:
                writer.writerow(row)

    def is_current_month(self, year, month):
            """检查给定的年份和月份是否是当前月份"""
            current_date = datetime.datetime.now()
            return year == current_date.year and month == current_date.month

    def fetch_and_save_stock_day(self, stock_id, year, month):
        filename = f'day/{stock_id}/{year}-{month:02d}.csv'
        should_fetch = self.is_current_month(year, month) or not os.path.exists(filename)

        if should_fetch:
            url = urllib.parse.urljoin(TWSE_BASE_URL, 'exchangeReport/STOCK_DAY')
            params = {'date': f'{year}{month:02d}01', 'stockNo': stock_id, 'response': 'json'}
            data = self.fetch_data(url, params)
            
            if data and data['stat'] == 'OK':
                new_data = [StockData(*row) for row in data['data']]
                self.save_as_csv(new_data, filename)
                print(f"Saved daily data for {stock_id} in {filename}")
            else: 
                print(f'error daily {stock_id} year: {year}, month: {month}')
    def fetch_and_save_yield(self, stock_id, year, month):
        filename = f'month/{stock_id}/{year}-{month:02d}_yield.csv'
        should_fetch = self.is_current_month(year, month) or not os.path.exists(filename)

        if should_fetch:
            url = urllib.parse.urljoin(TWSE_BASE_URL, 'exchangeReport/BWIBBU')
            params = {'date': f'{year}{month:02d}01', 'stockNo': stock_id, 'response': 'json'}
            data = self.fetch_data(url, params)

            if data and data['stat'] == 'OK':
                new_data = [YieldData(stock_id, row[0], row[1], row[2], row[3], row[4] if len(row) > 4 and row[4] else "N/A") for row in data['data']]
                self.save_as_csv(new_data, filename)
                print(f"Saved yield data for {stock_id} in {filename}")
            else: 
                print(f'error {stock_id} year: {year}, month: {month}')

    def read_stock_ids(self,filename):
        with open(filename, 'r') as file:
            stock_ids = file.read().splitlines()
        return stock_ids
    def fetch_stock_data(self,stock_id):
        for year in range(2012, 2024):
            for month in range(1, 13):
                print(f'{stock_id} year: {year}, month: {month}')
                if datetime.date(year, month, 1) <= datetime.date.today():
                    fetcher.fetch_and_save_stock_day(stock_id, year, month)
                    fetcher.fetch_and_save_yield(stock_id, year, month)
    def fetch_stock_data_with_semaphore(self,semaphore, stock_id):
        with semaphore:
            fetcher.fetch_stock_data(stock_id)
fetcher = TWSEFetcher()
stock_ids = fetcher.read_stock_ids('stock_num.txt')
threads = []
import threading
import datetime
# 為每個股票代碼創建一個執行緒
semaphore = threading.Semaphore(10)  # 最多允许10个线程同时运行

# 创建并启动线程
for stock_id in stock_ids:
    thread = threading.Thread(target=fetcher.fetch_stock_data_with_semaphore, args=(semaphore, stock_id))
    threads.append(thread)
    thread.start()

# 等待所有线程完成
for thread in threads:
    thread.join()

print("所有股票數據抓取完成")