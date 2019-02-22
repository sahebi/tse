import re
import ast
import json
import requests
import numpy as np
import mysql.connector

from tqdm import tqdm
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

    def execute(self, SQL):
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

    def insertInstrumentCode(self):
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

    def _getSymbolHistoryExtract(self, row):
        instrument_id = row[0]
        dt            = row[1]
        instrument_dt = [instrument_id, dt]
        # print(instrument_dt)

        # url = "data/symbolHistory.html"
        # file = open(url, 'r')
        # text = file.read()
        # file.close()

        try:
            url = f"http://cdn.tsetmc.com/Loader.aspx?ParTree=15131P&i={row[0]}&d={row[1]}"
            # url = f"http://cdn.tsetmc.com/Loader.aspx?ParTree=15131P&i=59142194115401696&d={row[1]}"
            # url = "http://cdn.tsetmc.com/Loader.aspx?ParTree=15131P&i=3493306453706327&d=20190206"
            # url = "http://cdn.tsetmc.com/Loader.aspx?ParTree=15131P&i=10145129193828624&d=20190210"
            # url = "http://cdn.tsetmc.com/Loader.aspx?ParTree=15131P&i=10145129193828624&d=20190209"
            # url = "http://cdn.tsetmc.com/Loader.aspx?ParTree=15131P&i=1072964149653157&d=20190205"

            r    = GetRequest(headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36"})
            text = r.get(url, {'Referer': "http://tse.ir/instrument/%D9%84%D8%A7%D8%A8%D8%B3%D8%A71_IRO1ASAL0001.html"})
            print(f"{row[2]} - {url}")
            # print(f"Text Length: {len(text)}")

            # MWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMW
            # SimpleData
            InstSimpleData = self.getRegex(text, r"<script>var InstSimpleData=\[(.*)\];var LVal18AFC")[0]
            InstSimpleData = ast.literal_eval(InstSimpleData)

            ClientTypeData = self.getRegex(text, r"var ClientTypeData=\[(.*)\];")[0]
            ClientTypeData = list(ClientTypeData.split(','))

            clientTypeColumns = {
                        10: 'buy_haghighi',
                        11: 'buy_hoghoghi',
                        12: 'sell_haghighi',
                        13: 'sell_hoghoghi',

                        14: 'buy_vol_haghighi',
                        15: 'buy_vol_hoghoghi',
                        16: 'sell_vol_haghighi',
                        17: 'sell_vol_hoghoghi',

                        18: 'buy_percent_haghighi',
                        19: 'buy_percent_hoghoghi',
                        20: 'sell_percent_haghighi',
                        21: 'sell_percent_hoghoghi',

                        22: 'buy_value_haghighi',
                        23: 'buy_value_hoghoghi',
                        24: 'sell_value_haghighi',
                        25: 'sell_value_hoghoghi',

                        26: 'buy_avg_price_haghighi',
                        27: 'buy_avg_price_hoghoghi',
                        28: 'sell_avg_price_haghighi',
                        29: 'sell_avg_price_hoghoghi',

                        30: 'change_percent_haghighi_hoghoghi',
                    }

            if(len(ClientTypeData) < 21):
                clientTypeColumns = {}

            data = list(InstSimpleData) + ClientTypeData

            columns = {
                            -2: 'instrument_code',
                            -1: 'dt',
                            2: 'col_2', 
                            4: 'flow', 
                            5: 'col_5',
                            6: 'col_6',
                            7: 'col_7',
                            8: 'SHARE_KOL', 
                            9: 'VOLUME_BASE',
                    }
            columns.update(clientTypeColumns)

            seprator = ''
            strCols  = ''
            strVal   = ''
            vals     = instrument_dt
            for i in columns:
                strCols += seprator + columns[i]
                strVal  += seprator + '%s'
                seprator = ','
                if(i > 0):
                    vals.append(data[i])

            SQL = f"insert into instrument_dt ({strCols}) values ({strVal})"
            self.db.insert(SQL, vals)

            # !!!! MWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMW
            # !!!! TreshholdData
            StaticTreshholdData = self.getRegex(text, r"<script>var StaticTreshholdData=\[(.*)\];")[0]
            StaticTreshholdData = ast.literal_eval(StaticTreshholdData)
            # print(row[0], "== StaticTreshholdData",len(StaticTreshholdData))
            # var StaticTreshholdData =[
            #                 قیمت مجاز
            #             ---------------
            #     [    1, 1097.00, 993.00],
            #     [60124, 1097.00, 993.00]
            # ];

            # MWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMW
            # Closing Price
            ClosingPriceData = self.getRegex(text, r"var ClosingPriceData=\[(.*)\];")[0]
            if len(ClosingPriceData) != 0:
                ClosingPriceData = ast.literal_eval(ClosingPriceData)
                ClosingPriceData = np.array(ClosingPriceData)
                if len(ClosingPriceData.shape) == 1:
                    ClosingPriceData = np.array([ClosingPriceData])

                datetime = np.char.split(ClosingPriceData[:,0])
                time_string = []
                time_value  = []
                for itm in datetime:
                    time_string.append(itm[1])
                    time_value.append(int(itm[1][0:2])*3600 + int(itm[1][3:5]) * 60 + int(itm[1][6:8]))

                ClosingPriceData = np.c_[np.repeat(instrument_id, ClosingPriceData.shape[0]),  
                                        np.repeat(dt, ClosingPriceData.shape[0]),
                                        time_string,
                                        time_value,
                                        ClosingPriceData] 
                ClosingPriceData = ClosingPriceData.tolist()
            
                columns = ['instrument_code', 'dt', 'time_string', 'time_value', 'date_time_sh', 'col_2', 'last_price', 'last_price_close', 'first_price_close', 'price_yesterday', 'high', 'low', 'id', 'COL_10', 'COL_11', 'COL_12', 'time_number']
                seprator = ''
                strCols  = ''
                strVal   = ''
                for col in columns:
                    strCols += seprator + col
                    strVal  += seprator + '%s'
                    seprator = ','

                SQL = f"insert into close_price ({strCols}) values ({strVal})"

                for rec in ClosingPriceData:
                    self.db.insert(SQL, rec)

            # MWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMWMW
            # Intraday Price
            IntraDayPriceData = self.getRegex(text, r"var IntraDayPriceData=\[(.*)\];")[0]
            if len(IntraDayPriceData) != 0:
                IntraDayPriceData = ast.literal_eval(IntraDayPriceData)
                
                if not type(IntraDayPriceData[0]) is list:
                    IntraDayPriceData = [IntraDayPriceData]

                IntraDayPriceData = np.array(IntraDayPriceData)
                
                datetime = IntraDayPriceData[:,0]
                time_string = []
                time_value  = []
                for itm in datetime:
                    time_string.append(int(itm[0:2])*10000 + int(itm[3:5]) * 100)
                    time_value.append(int(itm[0:2])*3600 + int(itm[3:5]) * 60)

                IntraDayPriceData = np.c_[  np.repeat(instrument_id, IntraDayPriceData.shape[0]),  
                                            np.repeat(dt, IntraDayPriceData.shape[0]),
                                            time_string,
                                            time_value,
                                            IntraDayPriceData
                                        ]

                for rec in IntraDayPriceData:
                    recStr = "'" + "','".join(map(str, rec)) + "'"
                    SQL = f"insert into instraday (instrument_code, dt, time_string, time_value, time_number, high, low, open, close, volume) values ({recStr})"
                    self.db.execute(SQL)

            InstrumentStateData = self.getRegex(text, r"var InstrumentStateData=\[(.*)\];")[0]
            InstrumentStateData = ast.literal_eval(InstrumentStateData)
            print(row[0], "InstrumentStateData", len(InstrumentStateData))
            # [[20170820,1,'A ']];
            return

            IntraTradeData = self.getRegex(text, r"var IntraTradeData=\[(.*)\];")[0]
            IntraTradeData = ast.literal_eval(IntraTradeData)
            print(row[0], "IntraTradeData", len(IntraTradeData))
            # ['1', '09:00:14', '2830','1045',0],
            # ['2', '09:08:18','29400','1040',0],
            # ['3', '09:12:12', '1000','1040',0],
            # ['4', '09:12:24',   '10','1040',0],

            ShareHolderData = self.getRegex(text, r"var ShareHolderData=\[(.*)\];")[0]
            ShareHolderData = ast.literal_eval(ShareHolderData)
            print(row[0], "ShareHolderData", len(ShareHolderData))
			# [
			# 	44141, 						**SHAREHOLDER_ID**
			# 	'IRO1BMLT0007',				**SYMBOL_UID**
			# 	8499999996, 				**SHARE**
			# 	16.990, 					**SHARE_PERCENT**
			# 	'', 						** SHARE_CHANGE [ArrowUp, Arrow??, ???] **
			# 	'دولت جمهوري اسلامي ايران' **SHAREHOLDER_NAME**
			# ],

            ShareHolderDataYesterday = self.getRegex(text, r"var ShareHolderDataYesterday=\[(.*)\];")[0]
            ShareHolderDataYesterday = ast.literal_eval(ShareHolderDataYesterday)
            print(row[0], "ShareHolderDataYesterday",len(ShareHolderDataYesterday))
			# 44141, 	'IRO1BMLT0007',8499999996,16.990,'','دولت جمهوري اسلامي ايران'
			# 965,  	'IRO1BMLT0007', 2499999999,4.990,'','صندوق تامين آتيه كاركنان بانك ملت'


            BestLimitData = self.getRegex(text, r"var BestLimitData=\[(.*)\];")[0]
            BestLimitData = ast.literal_eval(BestLimitData)
            print(row[0], "BestLimitData",len(BestLimitData))
            # 			    Buyer               Seller
            # 			-----------------  -----------------
            # 		Seq cnt    vol    price  price     vol   cnt
            # [60124,	'1','1',  '3000','1031','1050',  '3000','1'],
            # [60124,	'2','1',  '5000','1022','1055',  '3000','1'],
            # [60124,	'3','3', '86998','1021','1060', '23000','2'],
            # [60124,	'4','2','101000','1020','1065',  '3000','1'],
        except Exception as e:
            print(f"Error Extract Data:\nText Len:\n{e}")

    def getSymbolHistory(self):
        rows = tse.db.query("SELECT instrument_code, dt, instrument_history_id FROM `instrument_history` where instrument_history_id <= 50")

        pool = ThreadPool(self.thread_number)
        pool.map(self._getSymbolHistoryExtract, rows)

        pool.close()
        pool.join()


tse = TSE(thread_number=25)

# Update Instrument Table
# tse.insertInstrumentCode()
# tse.updateInstrument()
# tse.updateBasicInfo()

# Instrument_History:
#   from instrument where status = 'A'
# tse.updateInformation()

# from instrument_history:
#  instrument_dt:
#  close_price  :
tse.getSymbolHistory()






