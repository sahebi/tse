import requests
import json
from tqdm import tqdm
from bs4 import BeautifulSoup
import mysql.connector
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

class Database(object):
    def __init__(self,host="localhost",username="root",password="",database="tehran_stock_exchange"):
        self.host     = host
        self.username = username
        self.password = password
        self.database = database
        # self.connect()

    def connect(self):
        self.connection = mysql.connector.connect(
                                            host=self.host, 
                                            user=self.username, 
                                            passwd=self.password, 
                                            database=self.database)
    def insert(self, SQL, val):
        connection = mysql.connector.connect(
                                            host=self.host, 
                                            user=self.username, 
                                            passwd=self.password, 
                                            database=self.database)
        cursor = connection.cursor()
        cursor.execute(SQL, val)
        connection.commit()
        return cursor.getlastrowid

    def update(self, SQL):
        connection = mysql.connector.connect(
                                            host=self.host, 
                                            user=self.username, 
                                            passwd=self.password, 
                                            database=self.database)
        cursor = connection.cursor()
        cursor.execute(SQL)
        connection.commit()
        return cursor.rowcount

    def select(self, tablename, where="1=1", columns="*"):
        connection = mysql.connector.connect(
                                            host=self.host, 
                                            user=self.username, 
                                            passwd=self.password, 
                                            database=self.database)
        cursor = connection.cursor()
        SQL = f"select {columns} from {tablename} Where {where}"
        cursor.execute(SQL)
        return cursor.fetchall()

    # ________________________________________________________________________________
    def insert_instrument_code(self, val):
        try:            
            SQL_INSTRUMENT = "INSERT INTO instrument (instrument_code, ISIN, symbol, company_name) values (%s, %s, %s, %s)"
            return self.insert(SQL_INSTRUMENT, val)
        except:
            return -1
    # ________________________________________________________________________________
    def insert_history(self, val):
        try:            
            SQL_INSTRUMENT = "INSERT INTO instrument_history (instrument_code,dt,high,low,price_last,price_last_contract,open,price_yesterday,value,volume,cnt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            return self.insert(SQL_INSTRUMENT, val)
        except Exception as e:
            print(e)
            return -1
    # ________________________________________________________________________________
    def insert_instrument(self, val):
        try:
            columns = ['company_name', 'symbol', 'status', 'ISIN']

            seprator  = ""
            updateSet = ""
            for i in range(len(val)):
                updateSet += seprator + columns[i] + '="' + val[i] + '"'
                seprator = ","

            SQL = f"Update instrument Set {updateSet} where ISIN = '{val[3]}'"
            # print(SQL)
            return self.update(SQL)

        except:
            return -1
    def select_instrument(self):
        return self.select('instrument', columns='ISIN')

    # ________________________________________________________________________________
    def insert_basicinfo(self, val):
        try:
            columns = ['ISIN','company_name','symbol','company_name_en','symbol_en','CISIN','tableau','industry','industry_code','subindustry','subindustry_code']

            seprator  = ""
            updateSet = ""
            for i in range(len(val)):
                updateSet += seprator + columns[i] + '="' + val[i] + '"'
                seprator = ","

            SQL = f"Update instrument Set {updateSet} where ISIN = '{val[0]}'"
            # print(SQL)
            return self.update(SQL)
        except:
            return -1
        
class TSE(object):
    """ 
    """
    def __init__(self, database="tehran_stock_exchange", thread_number=50):
        self.thread_number = thread_number
        self.db = Database(database=database)

    def updateInstrumentURI(self,uri):
        uri_instrument = uri
        r = requests.get(uri_instrument)
        if(r.status_code == 200):
            js = json.loads(r.text)

        for alphabet in tqdm(range(len(js['companies']))):
            for idx in range(len(js['companies'][alphabet]['list'])):
                name   = js['companies'][alphabet]['list'][idx]['n']
                symbol = js['companies'][alphabet]['list'][idx]['sy']
                status = js['companies'][alphabet]['list'][idx]['s']
                ISIN   = js['companies'][alphabet]['list'][idx]['ic']
                val    = (name, symbol, status, ISIN)
                self.db.insert_instrument(val)

    def updateInstrument(self):
        self.updateInstrumentURI('http://tse.ir/json/Listing/ListingByName1.json')
        self.updateInstrumentURI('http://tse.ir/json/Listing/ListingByName2.json')
        self.updateInstrumentURI('http://tse.ir/json/Listing/ListingByName3.json')
        self.updateInstrumentURI('http://tse.ir/json/Listing/ListingByName4.json')
        self.updateInstrumentURI('http://tse.ir/json/Listing/ListingByName5.json')
        self.updateInstrumentURI('http://tse.ir/json/Listing/ListingByName7.json')

    def updateBasicInfo(self):
        for i, x in enumerate(tqdm(self.db.select_instrument())):
            ISIN           = x[0]
            uri_basicinfo  = f"http://tse.ir/json/Instrument/BasicInfo/BasicInfo_{ISIN}.html"
            try:
                r = requests.get(uri_basicinfo)
                if(r.status_code == 200):
                    r.encoding = r.apparent_encoding
                    soup = BeautifulSoup(r.text, 'html.parser')
                    basic_info = [x.select('td:nth-of-type(2)')[0].get_text() for x in soup.select('table:nth-of-type(1) tbody tr')]
                    self.db.insert_basicinfo(basic_info)
            except Exception as e:
                print(f"Error updateBasicInfo: {ISIN} {e}")

    def updateInstrumentCode(self):
        uri = 'http://www.tsetmc.com/tsev2/data/MarketWatchInit.aspx?h=0&r=0'
        try:
            r = requests.get(uri)
            if(r.status_code == 200):
                r.encoding = r.apparent_encoding
                csv = r.text
                rows = csv.split(';')
                for i, row in enumerate(tqdm(rows)):
                    if i == 0:
                        continue
                    cols = row.split(',')

                    if(len(cols[1]) > 3):
                        record = (cols[0], cols[1], cols[2], cols[3])
                        self.db.insert_instrument_code(record)
        except Exception as e:
            print(f"Error updateInstrumentCode: {e}")

    def openUrlAndExecuteInfirmation(self, ISIN):
        url = f"http://members.tsetmc.com/tsev2/data/InstTradeHistory.aspx?i={ISIN}&Top=99999999&A=1"
        r = requests.get(url)
        if r.status_code == 200:
            records = r.text.split(';')
            for j, record in enumerate(tqdm(records)):
                rec = record.split('@')
                if len(rec) != 10:
                    continue
                rec = [ISIN] + rec
                self.db.insert_history(rec)
        return ISIN

    def updateInformation(self):
        rows = self.db.select('instrument', where='status="A"', columns='instrument_code')
        urls = [row[0] for i, row in enumerate(tqdm(rows))]

        pool    = ThreadPool(self.thread_number)
        results = pool.map(self.openUrlAndExecuteInfirmation, urls)

        pool.close()
        pool.join()

        print(results)

tse = TSE(thread_number=200)
# tse.updateInstrumentCode()
# tse.updateInstrument()
# tse.updateBasicInfo()
# tse.updateInformation()