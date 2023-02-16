
from calendar import month
from io import BufferedWriter
from msilib.schema import Error
import requests
import time
import datetime
import urllib.parse
from collections import namedtuple
import codecs

from twstock.proxy import get_proxies
import urllib3

from SubSqlControl import SubSqlControl

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError
from decimal import Decimal

import pymysql.cursors
TWSE_BASE_URL = 'https://www.twse.com.tw'

DATATUPLE = namedtuple('股票殖利率', ['證券代號', '證券名稱', '殖利率', '股利年度', '本益比', '股價淨值比', '財報年'])




class BaseFetcher(object):
    def fetch(self, year, month, sid, retry):
        pass

    def _convert_date(self, date):
        """Convert '106/05/01' to '2017/05/01'"""
        return '/'.join([str(int(date.split('/')[0]) + 1911)] + date.split('/')[1:])

    def _make_datatuple(self, data):
        pass

    def purify(self, original_data):
        pass
class InsertData(object):
    type = 0
    def insert(self):
        pass
class InserYied(InsertData):
    type = 0
    def insert(self):
        
        pass
class InsertDaily(InsertData):
    type = 1
    def insert(self):
        pass

class TWSEYied(BaseFetcher):
    #https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=json&date=20220105&selectType=01
    #https://www.twse.com.tw/exchangeReport/BWIBBU?response=json&date=20220210&stockNo=1101&_=1644503241062
    #https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=json&date=&selectType=&_=1644410706139
    
    sqlControl = SubSqlControl()
    

    def __init__(self):
        pass
    #檢查是否有最新殖利率
    def checkNewYield(self):
        REPORT_URL = urllib.parse.urljoin(
        TWSE_BASE_URL, 'exchangeReport/BWIBBU')
        _year = 2023
        _month = 1
        lines = self.sqlControl.selectStockNumdidntCheck()
        data_headers = {'UserAgent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36','Content-type': 'application/json', 'Accept': 'text/plain'}
        for line in lines :

            params = {'date':  '%d%02d01' % (_year, _month) ,'response': 'json','stockNo':line[0] ,'_':''}  
            for retry_i in range(5):
                print(params)
                r = requests.get(REPORT_URL, params=params, headers=data_headers,
                proxies=get_proxies())
                            
                try:
                    data = r.json()
                except JSONDecodeError:
                    time.sleep(5)
                    continue
                except ConnectionError:
                    time.sleep(5)
                    continue
                except urllib3.exceptions.MaxRetryError:
                    time.sleep(5)
                    continue
                else:
                    break   
            if data['stat'] == 'OK':
                self.sqlControl.insertCheckFlag(line[0],1)     
            else :
                self.sqlControl.insertCheckFlag(line[0],0)
            time.sleep(5)    
    def fetchBWIBBU(self,retry: int=5):
        year = 2012
        month = 1
        #with open('D:\\stock\\twstock\\twstock\\stock_num.txt') as f:
        lines = self.sqlControl.selectStockNum()
        
        REPORT_URL = urllib.parse.urljoin(
        TWSE_BASE_URL, 'exchangeReport/BWIBBU')
        for line in lines :
            for _year in range(year,2023):
                for _month in range(month,13):
                    if datetime.datetime.strptime('%d-%02d-01' % (_year, _month),'%Y-%m-%d') > datetime.datetime.today() :
                        break
                    if self.sqlControl.checkDateData(0,line[0],'%d%02d01' % (_year, _month),'%d%02d30' % (_year, _month)) == 0:
                        params = {'date':  '%d%02d01' % (_year, _month) ,'response': 'json','stockNo':line[0] ,'_':''}
                        for retry_i in range(retry):
                            print(params)
                            try:
                                r = requests.get(REPORT_URL, params=params,
                                    proxies=get_proxies())                            
                                data = r.json()                             
                            except JSONDecodeError:
                                time.sleep(5)
                                continue
                            except ConnectionError:
                                time.sleep(5)
                                continue
                            except urllib3.exceptions.MaxRetryError:
                                time.sleep(5)
                                continue
                            except:
                                time.sleep(5)
                                continue
                            else:
                                break
                        if data['stat'] == 'OK':
                            self.sqlControl.insertdialyYield(line[0],data)
                        time.sleep(5) 
    
    def fetchStockDay(self,retry: int=5):
        REPORT_URL = urllib.parse.urljoin(
            TWSE_BASE_URL, 'exchangeReport/STOCK_DAY')     
        year = 2012
        month = 1
        #with open('D:\\stock\\twstock\\twstock\\stock_num.txt') as f:
        lines = self.sqlControl.selectStockNum()
        for line in lines :
            for _year in range(year,2023):
                for _month in range(month,13):
                    if datetime.datetime.strptime('%d-%02d-01' % (_year, _month),'%Y-%m-%d') > datetime.datetime.today() :
                        break
                    if self.sqlControl.checkDateData(1,line[0],'%d%02d01' % (_year, _month),'%d%02d30' % (_year, _month)) == 0:
                        params = {'date':  '%d%02d01' % (_year, _month) ,'response': 'json','stockNo':line[0] ,'_':''}
                        for retry_i in range(retry):
                            print(params)
                            try:
                                r = requests.get(REPORT_URL, params=params,
                                    proxies=get_proxies())
                                data = r.json()
                            except JSONDecodeError:
                                time.sleep(5)
                                continue
                            except ConnectionError:
                                time.sleep(5)
                                continue
                            except urllib3.exceptions.MaxRetryError:
                                time.sleep(5)
                                continue
                            except:
                                time.sleep(5)
                                continue
                            else:
                                break
                        if data['stat'] == 'OK':
                            self.sqlControl.insertdialyStock(line[0],data)
                        time.sleep(5) 
    def getStockdata(self,mode,url,_year,_month,line):
        if self.sqlControl.checkDateData(mode,line,'%d%02d01' % (_year, _month),'%d%02d30' % (_year, _month)) == 0:
            params = {'date':  '%d%02d01' % (_year, _month) ,'response': 'json','stockNo':line ,'_':''}
            for retry_i in range(5):
                try:
                    r = requests.get(url, params=params,
                                    proxies=get_proxies())
                    data = r.json()            
                except JSONDecodeError:
                    time.sleep(5)
                    return 1
                except ConnectionError:
                    time.sleep(5)
                    return 1
                except urllib3.exceptions.MaxRetryError:
                    time.sleep(5)
                    return 1
                except:
                    time.sleep(5)
                    return 1
                else:
                    return -1
            if data['stat'] == 'OK':
                if(mode == 1):
                    self.sqlControl.insertdialyStock(line,data)
                else :
                    self.sqlControl.insertdialyYield(line,data)
                time.sleep(5)
                return 0             
    def fetch(self,retry: int=5):
        REPORT_URL = urllib.parse.urljoin(
            TWSE_BASE_URL, 'exchangeReport/STOCK_DAY')     
        year = 2012
        month = 1
        #with open('D:\\stock\\twstock\\twstock\\stock_num.txt') as f:
        lines = self.sqlControl.selectStockNumWhereFlag()
        for line in lines :
            for _year in range(year,2023):
                for _month in range(month,13):
                    if datetime.datetime.strptime('%d-%02d-01' % (_year, _month),'%Y-%m-%d') > datetime.datetime.today() :
                        break
    def _make_datatuple(self, data):
        return DATATUPLE(*data)

    def purify(self, original_data):
        return [self._make_datatuple(d) for d in original_data['data']]

   
