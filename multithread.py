import time
import requests
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

# urls = [
#         'http://tse.ir/json/Listing/ListingByName1.json', 
#         'http://tse.ir/json/Listing/ListingByName2.json', 
#         'http://tse.ir/json/Listing/ListingByName3.json', 
#         'http://tse.ir/json/Listing/ListingByName4.json', 
#         'http://tse.ir/json/Listing/ListingByName5.json'
#         ]
# results = map(requests.get, urls)
# print(list(results))

urls = [
    'http://www.python.org',
    'http://www.python.org/about/',
    'http://www.onlamp.com/pub/a/python/2003/04/17/metaclasses.html',
    'http://www.python.org/doc/',
    'http://www.python.org/download/',
    'http://www.python.org/getit/',
    'http://www.python.org/community/',
    'https://wiki.python.org/moin/',
    'http://planet.python.org/',
    'https://wiki.python.org/moin/LocalUserGroups',
    'http://www.python.org/psf/',
    'http://docs.python.org/devguide/',
    'http://www.python.org/community/awards/'
    # etc..
]
pool = ThreadPool(10)
results = pool.map(requests.get, urls)
pool.close()
pool.join()

print(results)