
def insert(sql):
    conn = mysql.connector.connect(host="localhost", user="root", passwd="", database="TSE")
    curr  = conn.cursor()

    sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    val = ("John", "Highway 21")
    val = [
            ('Peter', 'Lowstreet 4'),
            ('Amy', 'Apple st 652'),
            ('Hannah', 'Mountain 21'),
            ('Michael', 'Valley 345'),
            ('Sandy', 'Ocean blvd 2'),
            ('Betty', 'Green Grass 1'),
            ('Richard', 'Sky st 331'),
            ('Susan', 'One way 98'),
            ('Vicky', 'Yellow Garden 2'),
            ('Ben', 'Park Lane 38'),
            ('William', 'Central st 954'),
            ('Chuck', 'Main Road 989'),
            ('Viola', 'Sideway 1633')
        ]
    curr.execute(sql, val)

    conn.commit()

    print(curr.rowcount, "record inserted.")
    return curr.rowcount

def select():
    db = mysql.connector.connect(host="",user="",passwd="",database="")
    mycursor = db.cursor()
    mycursor.execute("SELECT * FROM customers")
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)

url = 'http://tse.ir/json/Listing/ListingByName1.json'
r = requests.get(url)
js = json.loads(r.text)

i = 0
for alphabet in tqdm(range(len(js['companies']))):
    for idx in range(len(js['companies'][alphabet]['list'])):
        i += 1
        if(i>1):
            break

        name   = js['companies'][alphabet]['list'][idx]['n']
        symbol = js['companies'][alphabet]['list'][idx]['sy']
        status = js['companies'][alphabet]['list'][idx]['s']
        ISIN   = js['companies'][alphabet]['list'][idx]['ic']

        text = ''
        basic_info_uri = f"http://tse.ir/json/Instrument/BasicInfo/BasicInfo_{ISIN}.html"
        try:
            r = requests.get(basic_info_uri)
            if(r.status_code == 200):
                r.encoding = r.apparent_encoding
                d          = pq(r.text)
                html       = d.html()
                
                # soup = BeautifulSoup(r.text, 'html.parser')
                # for tr in soup.find_all('td'):
                #     print(tr.get_text())

                # sel = Selector(text=basic_info_table)
                # text += "\n"+basic_info_table
            else:
                print(f'error info: ({ISIN}) ({name})')
        except:
            print(f'error comapny info: ({ISIN}) status: ({r.status_code})')

        # status_uri = f"http://service.tse.ir/api/CompanyState?instId={ISIN}"
        # # print(status_uri)
        # try:
        #     r          = requests.get(status_uri)
        #     if(r.status_code == 200):
        #         js_status  = json.loads(r.text)
        #         # print(r.status_code)
        #     else:
        #         print(f'error info: ({ISIN}) ({name})')
        # except:
        #     print(f'error comapny info: ({ISIN}) status: ({r.status_code})')

        # 
        # error comapny info: (IRO1SAJN0001) status: (200)
        # error comapny info: (IRO1ARDK0001) status: (200)
        # error comapny info: (IRO1SRMA0001) status: (200)
        # error comapny info: (IRO1ATDM0001) status: (200)
        # http://tse.ir/json/Instrument/BasicInfo/BasicInfo_IRO1ATDM0001.html
        # 
print(text)
