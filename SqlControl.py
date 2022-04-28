
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

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError
from decimal import Decimal

import pymysql.cursors

class SqlControl(object):

    def _convert_date(self, date):
        """Convert '106/05/01' to '2017/05/01'"""
        return '/'.join([str(int(date.split('/')[0]) + 1911)] + date.split('/')[1:])

    def connect(self):
                
        mydb = pymysql.connect(
            host="localhost",
            user="root",
            password="kk412221",
            database="stock_database"
        )
        return mydb
    def insertCheckFlag(self,stock_num,flag):
        mydb = self.connect()
        with mydb.cursor() as cursor:      
            sql = "REPLACE INTO `stock_states` (`stock_id`,`flag`) VALUES(%s,%s)"
            cursor.execute(sql,(stock_num,flag))          
            mydb.commit()    

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
        try:
            with mydb.cursor() as cursor:
                for data in datas['data']:
                    data[0] = self._convert_date(self.convert_date(data[0]))
                    if len(data) == 4:
                        sql = "REPLACE INTO `stock_daily` (`stock_id_date`,`stock_id`,`date`,`yield`,`pe`,`value`) VALUES(%s,%s,%s,%s,%s,%s)"
                        cursor.execute(sql,(self.formatKey(stock_num,data[0]),stock_num,data[0],self.convert_data(data[1]),self.convert_data(data[2]),self.convert_data(data[3])))          
                    else :
                        sql = "REPLACE INTO `stock_daily` (`stock_id_date`,`stock_id`,`date`,`yield`,`pe`,`value`,`season`) VALUES(%s,%s,%s,%s,%s,%s,%s)"
                        cursor.execute(sql,(self.formatKey(stock_num,data[0]),stock_num,data[0],self.convert_data(data[1]),self.convert_data(data[3]),self.convert_data(data[4]),data[5]))
                mydb.commit()  
        except:
           return 

    def convert_data(self,data):
        try :
            return Decimal(data)
        except:
            return 0    
    def convert_date(self,date):
        return date.replace('年','/').replace('月','/').replace('日','')
    def formatKey(self,stock_num,date):
        return str(stock_num)+date
        
    def checkDateData(self,stock_num,begin_date,end_date):
        mydb = self.connect()
        with mydb.cursor() as cursor: 
            cursor.execute("SELECT count(*) FROM stock_daily where stock_id = (%s) and date > (%s) and date < (%s)",(stock_num,begin_date,end_date))
            return cursor.fetchone()[0]  
   


           
   
    stock_ids = []
    #最新年度有殖利率的股票代碼
    def selectStockNumWhereFlag(self):
        if len(self.stock_ids) != 0:
            return self.stock_ids
        mydb = self.connect()
        with mydb.cursor() as cursor:
             cursor.execute("SELECT stock_id FROM stock_states where flag= (%s)",(1))
             stock_ids = cursor.fetchall()   
             return stock_ids
    #所有未確認是否有殖利率的股票代碼
    def selectStockNumdidntCheck(self):
      
        mydb = self.connect()
        with mydb.cursor() as cursor:
             cursor.execute("SELECT stock_number.stock_id from stock_number left join stock_states on stock_states.stock_id = stock_number.stock_id where stock_states.stock_id is null")
             stock_ids = cursor.fetchall()   
             return stock_ids
    #所有股票代碼
    def selectStockNum(self):
        mydb = self.connect()
        with mydb.cursor() as cursor:
             cursor.execute("SELECT stock_id FROM stock_number")
             stock_ids = cursor.fetchall()   
             return stock_ids