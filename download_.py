
from calendar import month
from io import BufferedWriter
from msilib.schema import Error
import requests
import time
import datetime
import urllib.parse
import urllib.request
from collections import namedtuple
import codecs
import urllib
from twstock.proxy import get_proxies
import urllib3

from SqlControl import SqlControl

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError
from decimal import Decimal

import pymysql.cursors
REPORT_URL ='https://www.tdisdi.com/cert-search/?no_cert_found=0&contact[dob]=1991-04-29&contact[first_name]=THI+THU+HUONG&contact[last_name]=DO&certification_id=1221084&user_id=571134&certification={"certification_id":1221084,"user_id":571134}&action=verification#search-complete'
#REPORT_URL = 'https://www.tdisdi.com/cert-search/?no_cert_found=0&contact[dob]=1991-04-29&contact[first_name]=THI+THU+HUONG&contact[last_name]=DO&certification_id=1229549&user_id=571134&certification={"certification_id":1229549,"user_id":571134}&action=verification'
data_headers = {'referer':'https://www.tdisdi.com/cert-search/?certsearch%5Bdob%5D=1991-04-29&certsearch%5Bfirst_name%5D=THI+THU+HUONG&certsearch%5Blast_name%5D=DO','user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36','Content-type': 'text/html; charset=UTF-8', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9','cookie':'hblid=T0NB90s9joCYH4xS064fV0HCYbaaDAbA;_ga=GA1.2.1046206926.1651068515;_gid=GA1.2.137400580.1651068515;olfsk=olfsk9994637732486977;_fbp=fb.1.1651068515075.1538370618;iti_session_key=fxuu9hm85g09lo0smlcy6zot;PHPSESSID=nm0ctlnvri7hsa8sf88nur2rvn;wcsid=uc3IG7ay0TKg7v4Z064fV0HabbmaADjk;_okdetect={"token":"16510745807820","proto":"about:","host":""};_ok=4756-934-10-4060;_okbk=cd5=available,cd4=true,vi5=0,vi4=1651074580988,vi3=active,vi2=false,vi1=false,cd8=chat,cd6=0,cd3=false,cd2=0,cd1=0,;_oklv=1651074663951,uc3IG7ay0TKg7v4Z064fV0HabbmaADjk'}
r = requests.get(REPORT_URL, headers=data_headers,
                proxies=get_proxies())

#html=urllib.urlopen(REPORT_URL)

#print html.read()
print(r.content)
                