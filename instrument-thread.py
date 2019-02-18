import re
import ast
import json
import requests
from tqdm import tqdm
import mysql.connector
from bs4 import BeautifulSoup
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

class GetRequest(object):
    def __init__(self, headers={}):
        self.headers = headers

    def get(self, url, headers):
        headers.update(self.headers)
        try:
            r = requests.get(url, headers=headers)
            if(r.status_code == 200):
                r.encoding = r.apparent_encoding
                return r.text
            return None
        except Exception as e:
            print(f"GetRequest: {e}")

class Database(object):
    def __init__(self, host="localhost", username="root", password="", database="tehran_stock_exchange"):
        self.host     = host
        self.username = username
        self.password = password
        self.database = database
        # self.connect()

    # def connect(self):
    #     self.connection = mysql.connector.connect(
    #                                         host=self.host, 
    #                                         user=self.username, 
    #                                         passwd=self.password, 
    #                                         database=self.database)
    def insert(self, SQL, val):
        connection = mysql.connector.connect(
                                            host=self.host, 
                                            user=self.username, 
                                            passwd=self.password, 
                                            database=self.database)
        cursor = connection.cursor()
        cursor.execute(SQL, val)
        connection.commit()
        connection.close()
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
        connection.close()
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
        
        result = cursor.fetchall()
        connection.close()
        return result

    def query(self, sql):
        connection = mysql.connector.connect(
                                            host=self.host, 
                                            user=self.username, 
                                            passwd=self.password, 
                                            database=self.database)
        cursor = connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        connection.close()
        return result

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
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36"
            }

    def updateInstrumentURI(self,uri):
        uri_instrument = uri
        headers            = self.headers
        headers['Referer'] = "http://tse.ir/listing.html"
        r = requests.get(uri_instrument, headers=headers)
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
                headers            = self.headers
                headers['Referer'] = "http://tse.ir/instrument/%D9%84%D8%A7%D8%A8%D8%B3%D8%A71_IRO1ASAL0001.html"
                r = requests.get(uri_basicinfo, headers=headers)
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
            headers            = self.headers
            headers['Referer'] = "http://www.tsetmc.com/"
            r                  = requests.get(uri, headers=headers)
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

    def _openUrlAndExecuteInfirmation(self, ISIN):
        url = f"http://members.tsetmc.com/tsev2/data/InstTradeHistory.aspx?i={ISIN}&Top=99999999&A=1"
        headers            = self.headers
        headers['Referer'] = "http://www.tsetmc.com/"
        r                  = requests.get(url, headers=headers)
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
        results = pool.map(self._openUrlAndExecuteInfirmation, urls)

        pool.close()
        pool.join()

        print(f'Finished :){len(results)}')
        # print(results)

    def getRegex(self, text, regex):
        pattern = re.compile(regex, re.UNICODE+re.MULTILINE)
        return pattern.findall(text)

    def _getSymbolHistoryExtract(self, url):
        url  = f"http://cdn.tsetmc.com/Loader.aspx?ParTree=15131P&i=59142194115401696&d={rows[0][1]}"
        r    = GetRequest(headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36"})
        text = r.get(url, {'Referer': "http://tse.ir/instrument/%D9%84%D8%A7%D8%A8%D8%B3%D8%A71_IRO1ASAL0001.html"})

        try:
            InstSimpleData = self.getRegex(text, r"<script>var InstSimpleData=\[(.*)\];var LVal18AFC")[0]
            InstSimpleData = ast.literal_eval(InstSimpleData)
            
            StaticTreshholdData = self.getRegex(text, r"<script>var StaticTreshholdData=\[(.*)\];")[0]
            StaticTreshholdData = ast.literal_eval(StaticTreshholdData)

            ClosingPriceData = self.getRegex(text, r"var ClosingPriceData=\[(.*)\];")[0]
            ClosingPriceData = ast.literal_eval(ClosingPriceData)

            IntraDayPriceData = self.getRegex(text, r"var IntraDayPriceData=\[(.*)\];")[0]
            IntraDayPriceData = ast.literal_eval(IntraDayPriceData)
            
            InstrumentStateData = self.getRegex(text, r"var InstrumentStateData=\[(.*)\];")[0]
            InstrumentStateData = ast.literal_eval(InstrumentStateData)

            IntraTradeData = self.getRegex(text, r"var IntraTradeData=\[(.*)\];")[0]
            IntraTradeData = ast.literal_eval(IntraTradeData)

            ShareHolderData = self.getRegex(text, r"var ShareHolderData=\[(.*)\];")[0]
            ShareHolderData = ast.literal_eval(ShareHolderData)

            ShareHolderDataYesterday = self.getRegex(text, r"var ShareHolderDataYesterday=\[(.*)\];")[0]
            ShareHolderDataYesterday = ast.literal_eval(ShareHolderDataYesterday)
            print(ShareHolderDataYesterday)

            ClientTypeData = self.getRegex(text, r"var ClientTypeData=\[(.*)\];")[0]
            ClientTypeData = ast.literal_eval(ClientTypeData)

            BestLimitData = self.getRegex(text, r"var BestLimitData=\[(.*)\];")[0]
            BestLimitData = ast.literal_eval(BestLimitData)
        except Exception as e:
            print(f"Extract Data:\nText Len:{len(text)}\n{e}")

    def getSymbolHistory(self):
        rows = tse.db.query("SELECT instrument_code, dt FROM `instrument_history` Where instrument_history_id <= 74")
        urls = [f"http://cdn.tsetmc.com/Loader.aspx?ParTree=15131P&i={row[0]}&d={row[1]}" for row in rows]

        pool    = ThreadPool(self.thread_number)
        pool.map(self._getSymbolHistoryExtract, urls)

        pool.close()
        pool.join()
            
tse = TSE(thread_number=10)
# tse.updateInstrumentCode()
# tse.updateInstrument()
# tse.updateBasicInfo()
# tse.updateInformation()
tse.getSymbolHistory()