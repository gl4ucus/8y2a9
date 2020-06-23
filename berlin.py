"""
# NOT AUTOMATED YET
This script scrapes berlin boerse to obtain a list of bond ISINs being traded there
"""

import requests
import re
import pandas as pd

# Need to automate next time
total_pages = 34

isins_list = []
names_list = []

for _ in range(1, total_pages+1):
    r = requests.get(
            "https://www.boerse-berlin.com/index.php/Bonds/Search?table_pi=" + str(_) + "&search[submit]=Suche&rowsperpage=500",
            headers={'User-Agent': 'Mozilla/5.0'})

    isins_in_page = re.findall(r'isin=(.*?)">', r.text)
    isins_in_page = isins_in_page[::2]
    isins_list = isins_list + isins_in_page

    names_in_page = re.findall(r'isin=.*?">(.*?)</a><', r.text)
    names_in_page = names_in_page[::2]
    names_list = names_list + names_in_page

df = pd.DataFrame(list(zip(isins_list, names_list)), columns  = ["ISIN", "NAME"])
df.index += 1
df.to_excel("berlin.xlsx")
print("Done")

