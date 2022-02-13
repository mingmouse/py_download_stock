
from calendar import month
from io import BufferedWriter
from msilib.schema import Error
from mysqlx import IntegrityError
import requests
import time
import datetime
import urllib.parse
from collections import namedtuple
import codecs

from twstock.proxy import get_proxies
import urllib3

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError


TWSE_BASE_URL = 'http://www.twse.com.tw/'

DATATUPLE = namedtuple('股票殖利率', ['證券代號', '證券名稱', '殖利率', '股利年度', '本益比', '股價淨值比', '財報年'])
import pymysql.cursors




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

class TWSEYied(BaseFetcher):
    #https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=json&date=20220105&selectType=01
    #https://www.twse.com.tw/exchangeReport/BWIBBU?response=json&date=20220210&stockNo=1101&_=1644503241062
    #https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=json&date=&selectType=&_=1644410706139
    

    def connect(self):
                
        mydb = pymysql.connect(
            host="localhost",
            user="root",
            password="kk412221",
            database="stock_database"
        )
        return mydb

    def __init__(self):
        pass

    def insertStocknum(self):
        mydb = self.connect()
        with mydb.cursor() as cursor:
            with  codecs.open('D:\\stock\\twstock\\twstock\\stock_num.txt','r','utf-8') as f:
                lines = f.readlines()
                for line in lines:
                    line = line.replace('\n','')
                    stock = line.split('\t')
                    stock_name = stock[0]
                    stock_num = stock[1].replace('\r','').zfill(4)
                    sql = "INSERT INTO `stock_number` (`stock_id`, `stock_name`) VALUES (%s, %s)"
                        
                    cursor.execute("SELECT COUNT(*) FROM stock_number WHERE stock_id = (%s)",(stock_num))
                    count = cursor.fetchone()[0]
                           
                    if count == 0:
                        cursor.execute(sql,(stock_num,stock_name))
                    else :
                        cursor.execute(sql,(stock_num.zfill(6),stock_name))
                mydb.commit()   
                    
    def insertdialyYield(self,stock_num,datas):
        #print(datas)
        mydb = self.connect()
        with mydb.cursor() as cursor:
            for data in datas['data']:
                data[0] = self._convert_date(self.convert_date(data[0]))
                if len(data) == 4:
                    sql = "REPLACE INTO `stock_daily` (`stock_id_date`,`stock_id`,`date`,`yield`,`pe`,`value`) VALUES(%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql,(self.formatKey(stock_num,data[0]),stock_num,data[0],data[1],data[2],data[3]))          
                else :
                    sql = "REPLACE INTO `stock_daily` (`stock_id_date`,`stock_id`,`date`,`yield`,`pe`,`value`,`season`) VALUES(%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql,(self.formatKey(stock_num,data[0]),stock_num,data[0],data[1],data[3],data[4],data[5]))
            mydb.commit()   
    def insertCheckFlag(self,stock_num,flag):
        mydb = self.connect()
        with mydb.cursor() as cursor:      
            sql = "REPLACE INTO `stock_yield` (`stock_id`,`flag`) VALUES(%s,%s)"
            cursor.execute(sql,(stock_num,flag))          
            mydb.commit()           
    def convert_date(self,date):
        return date.replace('年','/').replace('月','/').replace('日','')
    def formatKey(self,stock_num,date):
        return str(stock_num)+date
    def selectStockNumWhereFlag(self):
        mydb = self.connect()
        with mydb.cursor() as cursor:
             cursor.execute("SELECT stock_id FROM stock_yield where flag= (%s)",(1))
             stock_ids = cursor.fetchall()   
             return stock_ids
    def selectStockNum(self):
        mydb = self.connect()
        with mydb.cursor() as cursor:
             cursor.execute("SELECT stock_id FROM stock_number")
             stock_ids = cursor.fetchall()   
             return stock_ids
    def checkNewYield(self):
        REPORT_URL = urllib.parse.urljoin(
        TWSE_BASE_URL, 'exchangeReport/BWIBBU')
        _year = 2022
        _month = 1
        lines = self.selectStockNum()
        for line in lines :
            params = {'date':  '%d%02d01' % (_year, _month) ,'response': 'json','stockNo':line[0] ,'_':''}  
            for retry_i in range(5):
                print(params)
                r = requests.get(REPORT_URL, params=params,
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
                self.insertCheckFlag(line[0],1)     
            else :
                self.insertCheckFlag(line[0],0)
    def fetchBWIBBU(self,retry: int=5):
        year = 2012
        month = 1
        #with open('D:\\stock\\twstock\\twstock\\stock_num.txt') as f:
        lines = self.selectStockNumWhereFlag()
        REPORT_URL = urllib.parse.urljoin(
        TWSE_BASE_URL, 'exchangeReport/BWIBBU')
        for line in lines :
            for _year in range(year,2022):
                for _month in range(month,12):
                    params = {'date':  '%d%02d01' % (_year, _month) ,'response': 'json','stockNo':line[0] ,'_':''}
                    for retry_i in range(retry):
                        print(params)
                        r = requests.get(REPORT_URL, params=params,
                            proxies=get_proxies())
                            
                        try:
                            data = r.json()
                            print(data)
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
                        self.insertdialyYield(line[0],data)
                    time.sleep(5)   

                  
    def fetchStockDay(self):
        REPORT_URL = urllib.parse.urljoin(
            TWSE_BASE_URL, 'exchangeReport/STOCK_DAY')     
                       
    def _make_datatuple(self, data):
        return DATATUPLE(*data)

    def purify(self, original_data):
        return [self._make_datatuple(d) for d in original_data['data']]

stock = TWSEYied()
stock.checkNewYield()
stock.fetchBWIBBU()
