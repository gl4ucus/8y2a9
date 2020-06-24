import pandas as pd
import aiohttp
import asyncio
import re

### Parameters ###
file_name = "1000.xlsx"
from_issue = 0
to_issue = 1000
##################

async def get(sem, url):
    async with sem:
        async with aiohttp.ClientSession() as session:
            async with session.get("http://cbonds.com/emissions/issue/" + str(url)) as response:
                content = await response.text()
                isin = re.findall(r'ISIN / ISIN RegS</td><td class="cb_text_right" width="50%">(.*?)</td></tr>', content)
                name = re.findall(r'<title>(.*?)</title>', content)
                status = re.findall(r'Issue ratings \(M/S&P/F\)</th></tr></thead><tbody><tr><td>(.*?)</td><td>', content)

                if len(isin) != 0:
                    isin = isin[0]
                else:
                    isin = None

                if len(name) != 0:
                    name = name[0]
                else:
                    name = None

                if len(status) != 0:
                    status = status[0]
                else:
                    status = None

            print(isin, name, status)
            return(url, isin, name, status)

sem = asyncio.Semaphore(10)

loop = asyncio.get_event_loop()
coroutines = [get(sem, _) for _ in range(from_issue, to_issue)]
results = loop.run_until_complete(asyncio.gather(*coroutines))

df = pd.DataFrame(results, columns = ["Issue", "Isin", "Name", "Status"])
df.to_excel(file_name)