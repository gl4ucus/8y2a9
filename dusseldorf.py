import requests
import pandas as pd
import re
import asyncio
import aiohttp

bonds_urls = []

def get_info(bond_url):
    r = requests.get(bond_url, headers={'User-Agent': 'Mozilla/5.0'})
    ISIN = re.findall(r'ISIN: (.*?)</title>', r.text)[0]
    name = re.findall(r'<h1>(.*?)</h1>',r.text)[0]
    return([name, ISIN])

for i in range(1, 247):
    url = "http://www.boerse-duesseldorf.de/anleihen/detailsuche?page=" + str(i) + "&emittent_id%7Blookup%7D=&emittent_land_id=&emittent_id=&kupon_typ_id=&waehrung_id=&min_kupon%7Bprice%7D=&max_kupon%7Bprice%7D=&min_restlaufzeit=&max_restlaufzeit=&min_rendite=&max_rendite=&pari=&do=1"
    r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    bonds_urls_in_page = re.findall(r'<span class="wkn"><a href="(.*?)">', r.text)
    bonds_urls_in_page = ['http://www.boerse-duesseldorf.de' + i for i in bonds_urls_in_page]
    bonds_urls = bonds_urls + bonds_urls_in_page



async def get(sem, url):
    async with sem:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                content = await response.text()
                isin = re.findall(r'ISIN: (.*?)</title>', content)[0]
                name = re.findall(r'<h1>(.*?)</h1>',content)[0]
            return (isin, name)


sem = asyncio.Semaphore(50)

loop = asyncio.get_event_loop()
coroutines = [get(sem, _) for _ in bonds_urls]
results = loop.run_until_complete(asyncio.gather(*coroutines))

df = pd.DataFrame(results, columns=["ISIN", "NAME"])

df.to_csv('dusseldorf.csv', index= False)