"""
This script scrapes frankfurt boerse to obtain a list of bond ISINs being traded there
"""

import requests
import pandas as pd
import math
import re

# Retrieving 1000 at a time
limit = 1000

offset = 0

data = {"lang":"en","offset": offset,"limit": limit,"sorting":"TURNOVER","sortOrder":"DESC"}

r = requests.post("https://api.boerse-frankfurt.de/search/bond_search/total_count?lang=en", json = data)
number_of_bonds = int(r.text)

number_of_cycles = math.ceil(number_of_bonds / limit)

names_list = []
isins_list = []

for _ in range(0, number_of_cycles):
    r = requests.post('https://api.boerse-frankfurt.de/search/bond_search', json=data)
    names_in_page = re.findall(r'"name":{"originalValue":"(.*?)"', r.text)
    names_list = names_list + names_in_page
    isins_in_page = re.findall(r'"isin":"(.*?)"', r.text)
    isins_list = isins_list + isins_in_page

    offset = offset + limit
    data["offset"] = offset

df = pd.DataFrame(list(zip(isins_list, names_list)), columns  = ["ISIN", "NAME"])
df.index += 1
df.to_excel("frankfurt.xlsx")
print("Done")