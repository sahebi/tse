# http://docs.python-requests.org/en/master/
# https://parsel.readthedocs.io/en/latest/usage.html
import requests
import json
from tqdm import tqdm
# from tabulate import tabulate
url = 'http://tse.ir/json/Listing/ListingByName1.json'

r = requests.get(url)
js = json.loads(r.text)

for alphabet in tqdm(range(len(js['companies']))):
    for idx in range(len(js['companies'][alphabet]['list'])):
        name   = js['companies'][alphabet]['list'][idx]['n']
        symbol = js['companies'][alphabet]['list'][idx]['sy']
        status = js['companies'][alphabet]['list'][idx]['s']
        ISIN   = js['companies'][alphabet]['list'][idx]['ic']

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

        basic_info_uri = f"http://tse.ir/json/Instrument/BasicInfo/BasicInfo_{ISIN}.html"
        try:
            r = requests.get(basic_info_uri)
            if(r.status_code == 200):
                js_status  = json.loads(r.text)
            else:
                print(f'error info: ({ISIN}) ({name})')
        except:
            print(f'error comapny info: ({ISIN}) status: ({r.status_code})')
