

#!/usr/bin/env python

import asyncio
import logging
import json
import re
import os, random
from datetime import datetime
import concurrent.futures

import pandas as pd
from DataRecorder import Recorder
import time

# try:
#     import aiofiles
# except ImportError:
#     raise ImportError('missing required aiofiles library (pip install aiofiles)')

try:
    import aiohttp
except ImportError:
    raise ImportError("missing required aiohttp library (pip install aiohttp)")
import aiohttp
import asyncio
from contextlib import asynccontextmanager
from dbhelper import DatabaseManager
from sqlitebatch import  SQLiteBatchedWriter
# Usage
# Now you can use db_manager.add_screenshot(), db_manager.read_screenshot_by_url(), etc.
from loguru import logger
import threading
from queue import Queue

# Replace this with your actual test URL
test_url = "http://example.com"

# Replace this with your actual outfile object and method for adding data
# outfile = YourOutfileClass()
# Color codes
BLUE = "\033[1;34m"
CYAN = "\033[1;36m"
GREEN = "\033[1;32m"
GREY = "\033[1;90m"
PINK = "\033[1;95m"
PURPLE = "\033[0;35m"
RED = "\033[1;31m"
YELLOW = "\033[1;33m"
RESET = "\033[0m"

MAX_RETRIES = 3
INITIAL_DELAY = 1
MAX_DELAY = 10

# Setup basic logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Global variable to store RDAP servers
RDAP_SERVERS = {}
semaphore = threading.Semaphore(5)  # Allow up to 5 concurrent tasks
db_name='builtwith-topsite.db'
batched_writer = SQLiteBatchedWriter(db_name=db_name, batch_size=100)

# Shared dictionary to store results
shared_dict = {}

# Lock to ensure thread-safe access to the shared dictionary
lock = threading.Lock()





async def get_proxy():
    proxy = None
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("http://demo.spiderpy.cn/get") as response:
                data = await response.json()
                proxy = data["proxy"]
                return proxy
        except:
            pass


async def get_proxy_proxypool():
    async with aiohttp.ClientSession() as session:

        if proxy is None:
            try:
                async with session.get(
                    "https://proxypool.scrape.center/random"
                ) as response:
                    proxy = await response.text()
                    return proxy
            except:
                return None




def get_top(
    browser, domain: str, proxy_url: str, country: str,fanwei:str,fenye:int
):
    """
    Looks up a domain using the RDAP protocol.

    :param domain: The domain to look up.
    :param proxy_url: The proxy URL to use for the request.
    :param semaphore: The semaphore to use for concurrency limiting.
    """
    with semaphore:
        page = browser.new_tab()

        # page.get_tab(tab)

    # query_url='https://ahrefs.com/top'
        query_url = domain

        logger.info("use proxy_url:{}", proxy_url)

        logger.info("querying:{}", query_url)
        browser = None
        try:
            headless
        except:
            headless = True

        try:

            page.get(query_url)

            page.wait.load_start()
            try:
                maxpagi=page.ele('.pagination').texts
                max_int = max((x for x in maxpagi if isinstance(x, int)), default=None)

                maxpagi=max_int+1
            except:
                maxpagi=21

            if fenye==1:

                with lock:  # Ensure that only one thread can access the dictionary at a time
                    shared_dict[country+'_'+fanwei] = maxpagi  # Store the result in the shared dictionary using the key


                return maxpagi
            if fenye>maxpagi:
                logger.info('this pagi is not in the result')
                return 

            trs = (
            page.ele(".table table-responsive-sm table-sm mt-2 table-hover")
            .ele("tag:tbody")
            .children()
        )
            for tr in trs:
                text = []
                datahash=tr.attr('data-hash')
                text.append(datahash)
                for i in tr.children()[:-1]:
                    value=i.text
                    if i.text is None or value=='' or value=='-':
                        value=None
                    
                    text.append(value)

                text.append(country)
                text.append(fanwei)
                text.append(fenye)

            # browser.saveCookie(cookiepath)

            # data =','.join(text)
                outfile.add_data(text)
                # print(text)

                # batched_writer.add_data(tablename, columns, [text])
                # batched_writer.join()


            logger.info(
                f"{GREEN}SUCCESS {GREY}| {BLUE} {GREY}| {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain}{GREEN}"
            )

        except asyncio.TimeoutError as e:
            logger.info(
            f"{RED} TimeoutError {GREY}| --- | {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain} {RED}| {e}{RESET}"
        )
            raise

        except aiohttp.ClientError as e:
            logger.info(
            f"{RED} ClientError {GREY}| --- | {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain} {RED}| {e}{RESET}"
        )
            raise

        except Exception as e:
        # page.quit()
        # 需要注意的是，程序结束时浏览器不会自动关闭，下次运行会继续接管该浏览器。

        # 无头浏览器因为看不见很容易被忽视。可在程序结尾用page.quit()将其关闭 不 quit 还是会无头模式
        # headless=False

            logger.info(
            f"{RED}Exception  {GREY}| --- | {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain} {RED}| {e}{RESET}"
        )
            raise

        finally:
            page.close()
            print("finally")




# To run the async function, you would do the following in your main code or script:
# asyncio.run(test_proxy('your_proxy_url_here'))
def cleandomain(domain):
    domain = domain.strip()
    if "https://" in domain:
        domain = domain.replace("https://", "")
    if "http://" in domain:
        domain = domain.replace("http://", "")
    if "www." in domain:
        domain = domain.replace("www.", "")
    if domain.endswith("/"):
        domain = domain.rstrip("/")
    return domain

def getbuiltwithtopsitespaginations():
    from DPhelper import DPHelper

    browser = DPHelper(
        browser_path=None, HEADLESS=False, 
        # proxy_server="socks5://127.0.0.1:1080"
    ).driver
    concurrency = 5
    outdf=None    
    if os.path.exists(outfilepath):
        outdf=pd.read_csv(outfilepath)


    tasks = []
    countrylist = [
        "",
        "United-States",
        "United-Kingdom",
        "Canada",
        "Australia",
        "New-Zealand",
        "Germany",
        "France",
        "Netherlands",
        "Italy",
        "Spain",
        "Mexico",
        "India",
        "Japan",
        "Switzerland",
        "China",
        "Russia",
        "Sweden",
        "Norway",
        "Brazil",
        "Indonesia",
        "Turkey",
        "Saudi-Arabia",
        "Argentina",
        "Poland",
        "Belgium",
        "Thailand",
        "Austria",
        "Israel",
        "Hong-Kong",
        "Denmark",
        "Singapore",
        "Malaysia",
        "South-Korea",
        ".co",
        "Philippines",
        "Pakistan",
        "Chile",
        "Finland",
        "Vietnam",
        "Czech-Republic",
        "Romania",
    ]
    fanweis = ["", "/eCommerce"]
    detail = "https://pro.builtwith.com/ajax/meta.aspx?lang=en&DOM=samsung.com&HASH=fc1ebdfe-b52d-52a7-a76f-0c08a24852c6"
    for c in countrylist:

        url = f"https://builtwith.com/top-sites"
        country  = "global" if c=='' else c

        if c:
            url = f"https://builtwith.com/top-sites/{c}"
        for fanwei in fanweis:
            fenyelist=[]
            url = url + fanwei
            fanwei='all' if fanwei=='' else 'eCommerce'

            # fenyelist=batched_writer.query_sqlite(tablename,country, fanwei)
   
            proxy = None


            for i in range(1,2):
                domain=url+f"?PAGE={i}"
                try:

                    task = threading.Thread(target=get_top, args=(browser, domain, proxy, country,fanwei, i))
                    tasks.append(task)
                    task.start()

                except Exception as e:
                    print(f"{RED}An error occurred while processing {domain}: {e}")
    print(len(tasks),'===================')
    # time.sleep(60)

    for task in tasks:
        task.join()

    browser.quit()

        # page.close()

def getbuiltwithtopsites():
    from DPhelper import DPHelper

    browser = DPHelper(
        browser_path=None, HEADLESS=False, 
        # proxy_server="socks5://127.0.0.1:1080"
    ).driver
    concurrency = 5
    outdf=None    
    if os.path.exists(outfilepath):
        outdf=pd.read_csv(outfilepath)


    tasks = []
    countrylist = [
        "",
        "United-States",
        "United-Kingdom",
        "Canada",
        "Australia",
        "New-Zealand",
        "Germany",
        "France",
        "Netherlands",
        "Italy",
        "Spain",
        "Mexico",
        "India",
        "Japan",
        "Switzerland",
        "China",
        "Russia",
        "Sweden",
        "Norway",
        "Brazil",
        "Indonesia",
        "Turkey",
        "Saudi-Arabia",
        "Argentina",
        "Poland",
        "Belgium",
        "Thailand",
        "Austria",
        "Israel",
        "Hong-Kong",
        "Denmark",
        "Singapore",
        "Malaysia",
        "South-Korea",
        ".co",
        "Philippines",
        "Pakistan",
        "Chile",
        "Finland",
        "Vietnam",
        "Czech-Republic",
        "Romania",
    ]
    fanweis = ["", "/eCommerce"]
    detail = "https://pro.builtwith.com/ajax/meta.aspx?lang=en&DOM=samsung.com&HASH=fc1ebdfe-b52d-52a7-a76f-0c08a24852c6"
    for c in countrylist:

        url = f"https://builtwith.com/top-sites"
        country  = "global" if c=='' else c

        if c:
            url = f"https://builtwith.com/top-sites/{c}"
        for fanwei in fanweis:
            fenyelist=[]
            url = url + fanwei
            fanwei='all' if fanwei=='' else 'eCommerce'

            # fenyelist=batched_writer.query_sqlite(tablename,country, fanwei)
            if outdf is not None:
                outdfc=outdf[outdf['country'] == c]
                if outdfc is not None:
                    fenyelist=outdfc[outdfc['fenlei']==fanwei]['fenye'].to_list()


            proxy = None
            maxpagi=21
            file_path = 'builtwith-pagi.json'
            global shared_dict  # Declare shared_dict as global

                
            if  shared_dict and shared_dict!={}:
                with lock:  # Ensure thread-safe access to the shared dictionary
                    maxpagi=shared_dict.get(country+'_'+fanwei,21)
            if os.path.exists(file_path):
                shared_dict=json.load(open(file_path))
            for i in range(1,maxpagi):
                if fenyelist and  i in fenyelist:
                    print(f'{country}-{fanwei}-{i} done !')
                    continue
                domain=url+f"?PAGE={i}"
                try:

                    task = threading.Thread(target=get_top, args=(browser, domain, proxy, country,fanwei, i))

                    print(f'try to add:{country}-{fanwei}-{i} ')
                    tasks.append(task)
                    task.start()

                except Exception as e:
                    print(f"{RED}An error occurred while processing {domain}: {e}")
    print(len(tasks),'===================')
    # time.sleep(60)

    for task in tasks:
        task.join()

    browser.quit()

        # page.close()

def dumppagi():

    import json

    # # Your dictionary
    # shared_dict = {
    #     'key1': 'value1',
    #     'key2': 'value2',
    #     # ...
    # }

    # The path to the file where you want to save the dictionary
    file_path = 'builtwith-pagi.json'

    # Serialize the dictionary to a JSON formatted string
    # and write it to a file
    with open(file_path, 'w') as file:
        json.dump(shared_dict, file)
counts = 0
headless = True
cookiepath = "cookie.txt"
start = datetime.now()
folder_path = "."
# logger.add(f"{folder_path}/domain-index-ai.log")
# print(domains)
outfilepath = "builtwith-top.csv"
columns = ['datahash',
'rank','smalllogo','domain','location','salerev','techspend',"social","employee",'traffic','country','fenlei','fenye'
            ]
tablename = 'builtwith_top_sites'
# batched_writer.start(tablename,columns)
# time.sleep(30)

outfile = Recorder(folder_path + "/" + outfilepath, cache_size=5000)
if os.path.exists(outfilepath)==False:
    outfile.set.head(columns)

getbuiltwithtopsitespaginations()
dumppagi()
# getbuiltwithtopsites()
batched_writer.close()

end = datetime.now()
print("costing", end - start)
outfile.record()
