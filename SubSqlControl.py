
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

class SubSqlControl(object):

    def _convert_date(self, date):
        """Convert '106/05/01' to '2017/05/01'"""
        return '/'.join([str(int(date.split('/')[0]) + 1911)] + date.split('/')[1:])

    def convert_data(self,data):
        try :
            return Decimal(data)
        except:
            return 0    
    def convert_date(self,date):
        return date.replace('年','/').replace('月','/').replace('日','')
    def convert_vol(self,vol):
        try :
            return Decimal(vol.replace(',',''))
        except:
            return 0  
    def formatKey(self,stock_num,date):
        return str(stock_num)+date

    def connect(self):
                
        mydb = pymysql.connect(
            host="localhost",
            user="root",
            password="root",
            database="stock_database"
        )
        return mydb

    def insertCheckFlag(self,stock_num,flag):
        mydb = self.connect()
        with mydb.cursor() as cursor:
            sql = "INSERT INTO 'stock_states' (`stock_id`,`flag`) VALUES (%s,%s) ON DUPLICATE KEY UPDATE `stock_id` = %s, `flag` = %s"

            #sql = "REPLACE INTO `stock_states` (`stock_id`,`flag`) VALUES(%s,%s)"
            cursor.execute(sql,(stock_num,flag,stock_num,flag))          
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
                    
    def insert_daily_yield(self, stock_num, datas):
        mydb = self.connect()
        try:
            with mydb.cursor() as cursor:
                for data in datas['data']:
                    data[0] = self._convert_date(self.convert_date(data[0]))
                    if len(data) == 4:
                        sql = """
                            INSERT INTO `stock_daily_{0}`
                            (`stock_id_date`, `stock_id`, `date`, `yield`, `pe`, `value`)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                            `yield` = VALUES(`yield`),
                            `pe` = VALUES(`pe`),
                            `value` = VALUES(`value`)
                        """.format(stock_num)
                        values = (
                            self.formatKey(stock_num, data[0]),
                            stock_num,
                            data[0],
                            self.convert_data(data[1]),
                            self.convert_data(data[2]),
                            self.convert_data(data[3])
                        )
                    else:
                        sql = """
                            INSERT INTO `stock_daily_{0}`
                            (`stock_id_date`, `stock_id`, `date`, `yield`, `pe`, `value`, `season`)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                            `yield` = VALUES(`yield`),
                            `pe` = VALUES(`pe`),
                            `value` = VALUES(`value`),
                            `season` = VALUES(`season`)
                        """.format(stock_num)
                        values = (
                            self.formatKey(stock_num, data[0]),
                            stock_num,
                            data[0],
                            self.convert_data(data[1]),
                            self.convert_data(data[3]),
                            self.convert_data(data[4]),
                            data[5]
                        )

                    cursor.execute(sql, values)
                mydb.commit()
        except Exception as e:
            print(e)
    # 0: "日期"
    # 1: "成交股數" vol
    # 2: "成交金額"volnum
    # 3: "開盤價" open_price
    # 4: "最高價" max_price
    # 5: "最低價" min_price
    # 6: "收盤價" close_price
    # 7: "漲跌價差"
    # 8: "成交筆數"vol_x
    def insertdialyStock(self,stock_num,datas):
        #print(datas)
        mydb = self.connect() 
        try:
            with mydb.cursor() as cursor:
                for data in datas['data']:
                    data[0] = self._convert_date(self.convert_date(data[0]))
                    sql = "REPLACE INTO `stock_daily_data_%s`" % stock_num
                    conditional = " (`stock_id_date`,`stock_id`,`date`,`close_price`,`open_price`,`max_price`,`min_price`,`volnum`,`vol`,`vol_x`) VALUES('{0}','{1}','{2}',{3},{4},{5},{6},{7},{8},{9})".format(
                        self.formatKey(stock_num,data[0])
                        ,stock_num
                        ,data[0]
                        ,self.convert_data(data[6])
                        ,self.convert_data(data[3])
                        ,self.convert_data(data[4])
                        ,self.convert_data(data[5])
                        ,self.convert_vol(data[2])
                        ,self.convert_vol(data[1])
                        ,self.convert_vol(data[8]))
                    print("%s%s"%(sql,conditional))
                    cursor.execute("%s%s"%(sql,conditional))                              
                mydb.commit()  
        except Exception as e: 
            print(e)
            return 
    
        
    def checkDateData(self,mode,stock_num,begin_date,end_date):
        mydb = self.connect()
        with mydb.cursor() as cursor: 
            if(mode == 0) :
                sql = "SELECT count(*) FROM stock_daily_%s"%stock_num
                conditional = " where stock_id = '{0}' and date > '{1}' and date < '{2}'  ORDER BY DATE DESC LIMIT 1".format(stock_num,begin_date,end_date)
                print("%s%s"%(sql,conditional))
                #print("SELECT count(*) FROM stock_daily where stock_id = (%s) and date > (%s) and date < (%s)",(stock_num,begin_date,end_date))
                cursor.execute("%s%s"%(sql,conditional))
            else :
                sql = "SELECT count(*) FROM stock_daily_data_%s"%stock_num
                conditional = " where stock_id = '{0}' and date > '{1}' and date < '{2}'  ORDER BY DATE DESC LIMIT 1".format(stock_num,begin_date,end_date)
                #print()
                print("%s%s"%(sql,conditional))
                #print("SELECT count(*) FROM stock_daily_data where stock_id = (%s) and date > (%s) and date < (%s)",(stock_num,begin_date,end_date))
                cursor.execute("%s%s"%(sql,conditional))
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

    def createTableByStockNumber(self,stock_number):
        daliy_data_table_sql = " \
                CREATE TABLE IF NOT EXISTS `stock_daily_data_%s` ( \
                `stock_id_date` varchar(255) NOT NULL, \
                `stock_id` varchar(11) NOT NULL DEFAULT '0', \
                `date` date DEFAULT NULL, \
                `vol` double DEFAULT NULL, \
                `volnum` double DEFAULT NULL, \
                `open_price` double DEFAULT NULL,\
                `max_price` double DEFAULT NULL,\
                `min_price` double DEFAULT NULL,\
                `close_price` double DEFAULT NULL,\
                `vol_x` double DEFAULT NULL,\
                PRIMARY KEY (`stock_id_date`)\
                ) ENGINE=MyISAM DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;"

      
        mydb = self.connect()
        with mydb.cursor() as cursor:
            cursor.execute(daliy_data_table_sql % (stock_number,))
            mydb.commit()  
        return 0

    def createDailyTableByStockNumber(self,stock_number):
        
        stock_daliy = "\
                CREATE TABLE IF NOT EXISTS `stock_daily_%s` ( \
                `stock_id_date` varchar(255) NOT NULL, \
                `stock_id` varchar(11) NOT NULL DEFAULT '0',\
                `date` date DEFAULT NULL,\
                `yield` decimal(10,2) DEFAULT NULL,\
                `pe` decimal(10,2) DEFAULT NULL,\
                `value` decimal(10,2) DEFAULT NULL,\
                `season` varchar(255) DEFAULT NULL,\
                PRIMARY KEY (`stock_id_date`)\
                ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
       
        mydb = self.connect()
        with mydb.cursor() as cursor:
            cursor.execute(stock_daliy  % (stock_number,))
            mydb.commit()  
        return 0    

    def syncDataToSplitTable(self):
        #取得所有的股票id
        #從舊表取出數據 done 
        #創建新表(如果新表不存在) done 
            #for test onle , 將舊表數據打印.
        #將舊表數據同步至新表 

        lines = self.selectStockNum()
        for line in lines :
            line = line[0]
            self.createDailyTableByStockNumber(line)
            self.createTableByStockNumber(line)
            data =  self.selectDaliyTable(line)
            for _data in data:
                self.syncDataToTalbe(line,_data)
            data = self.selectDaliyData(line)     
            for _data in data:
                self.syncDataTodaily(line,_data) 
            print("done sync data %s" % line) 
            print("done create table %s" % line)
        #for line in lines:
                      
        return 0

    def selectDaliyData(self,stock_number):
        mydb = self.connect()
        with mydb.cursor() as cursor: 
            sql = "SELECT * FROM stock_daily where stock_id= '%s'"% stock_number
            print(sql)
            cursor.execute(sql) 
            return cursor.fetchall()

    def selectDaliyTable(self,stock_number):
        mydb = self.connect()
        with mydb.cursor() as cursor: 
            sql = "SELECT * FROM stock_daily_data where stock_id = '%s'"% stock_number 
            print(sql)
            cursor.execute(sql)    
            return cursor.fetchall() 

    def syncDataToTalbe(self,stock_number,datas):
        sql = "REPLACE INTO `stock_daily_data_'%s'"%  stock_number 
        sql = sql % " (`stock_id_date`,`stock_id`,`date`,`close_price`,`open_price`,`max_price`,`min_price`,`volnum`,`vol`,`vol_x`) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
       
        print(sql)
        mydb = self.connect()
        try:
            with mydb.cursor() as cursor:
               
                print(sql , (datas[0],datas[1],datas[2],self.convert_data(datas[3]),self.convert_data(datas[4]),self.convert_data(datas[5])
                ,self.convert_data(datas[6]),self.convert_vol(datas[7]),self.convert_vol(datas[8]),self.convert_vol(datas[9])))
                #cursor.execute(sql , (datas[0],datas[1],datas[2],self.convert_data(datas[3]),self.convert_data(datas[4]),self.convert_data(datas[5])
                #,self.convert_data(datas[6]),self.convert_vol(datas[7]),self.convert_vol(datas[8]),self.convert_vol(datas[9])))
                                                
                mydb.commit()  
        except Exception as e: 
            print(e)
        return 

    def syncDataTodaily(self,stock_number,datas):
        mydb = self.connect()
        sql = "REPLACE INTO `stock_daily_'%s'"%  stock_number 
        print(sql)
        try:
            with mydb.cursor() as cursor:
                if datas[6]:
                    
                    sql = sql % " (`stock_id_date`,`stock_id`,`date`,`yield`,`pe`,`value`,`season`) VALUES(%s,%s,%s,%s,%s,%s,%s)"
                    print(sql,(datas[0],datas[1],datas[2],self.convert_data(datas[3]),self.convert_data(datas[4]),self.convert_data(datas[5]),datas[6]))

                    cursor.execute(sql ,(datas[0],datas[1],datas[2],self.convert_data(datas[3]),self.convert_data(datas[4]),self.convert_data(datas[5]),datas[6]))
                else :
                    sql = sql % " (`stock_id_date`,`stock_id`,`date`,`yield`,`pe`,`value`) VALUES(%s,%s,%s,%s,%s,%s)"  
                    print(sql , (datas[0],datas[1],datas[2],self.convert_data(datas[3]),self.convert_data(datas[4]),self.convert_data(datas[5])))
                    cursor.execute(sql , (datas[0],datas[1],datas[2],self.convert_data(datas[3]),self.convert_data(datas[4]),self.convert_data(datas[5])))      
                    
                mydb.commit()  
        except Exception as e: 
            print(e)

        return 

