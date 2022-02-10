
from calendar import month
from io import BufferedWriter
import requests
import time
import datetime
import urllib.parse
from collections import namedtuple
import codecs

from twstock.proxy import get_proxies

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError


TWSE_BASE_URL = 'http://www.twse.com.tw/'

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

class TWSEYied(BaseFetcher):
    #https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=json&date=20220105&selectType=01
    #https://www.twse.com.tw/exchangeReport/BWIBBU?response=json&date=20220210&stockNo=1101&_=1644503241062
    #https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=json&date=&selectType=&_=1644410706139
    REPORT_URL = urllib.parse.urljoin(
        TWSE_BASE_URL, 'exchangeReport/BWIBBU')

    def __init__(self):
        pass
                        
    def fetch(self,retry: int=5):
        year = 2012
        month = 1
        with open('D:\\stock\\twstock\\twstock\\stock_num.txt') as f:
            lines = f.readline().replace('\n','')
           
            file = codecs.open('D:\\stock\\twstock\\twstock\\data\\'+str(lines)+'.txt','w','utf-8')
            for _year in range(year,2022):
                for _month in range(month,12):
                    params = {'date':  '%d%02d01' % (_year, _month) ,'response': 'json','stockNo':lines }
                   
                    for retry_i in range(retry):
                    
                        r = requests.get(self.REPORT_URL, params=params,
                                        proxies=get_proxies())
                        
                        try:
                            data = r.json()
                            print(data)
                        except JSONDecodeError:
                            continue
                        else:
                           break
                            
                   # if data['stat'] == 'OK':
                       # self._write_file(file,data['data'])
                    time.sleep(5)
                  
    #def _write_file(self,file:codecs.StreamReaderWriter,data):
       # for _data in data:
          #  print(_data)
            #file.write(_data)
           #file.write('\n')

    def _make_datatuple(self, data):
        return DATATUPLE(*data)

    def purify(self, original_data):
        return [self._make_datatuple(d) for d in original_data['data']]

stock = TWSEYied()
data = stock.fetch()