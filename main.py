from calendar import month
from io import BufferedWriter
#from msilib.schema import Error
import requests
import time
import datetime
import urllib.parse
from collections import namedtuple
import codecs

from twstock.proxy import get_proxies
import urllib3
from getYied import TWSEYied
from SubSqlControl import SubSqlControl

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError
from decimal import Decimal

import pymysql.cursors


stock = TWSEYied()
stock.create()
#while True :
    
    #stock.fetchBWIBBU()
