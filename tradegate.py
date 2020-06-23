"""
This script scrapes tradegate to obtain a list of bond ISINs being traded there
"""

import pandas as pd
import requests
import string
import re

price_lists = ['3','4','7'] + list(string.ascii_uppercase)
price_lists_urls = ["https://www.tradegate.de/indizes.php?lang=en&index=alle&buchstabe=" + i + "&art=anleihen" for i in price_lists]

isins_list = []
names_list = []

for price_lists_url in price_lists_urls:
    r = requests.get(price_lists_url)
    r = r.text.split("<tbody id=\"kursliste_abc\">")[1]
    r = r.split("</tbody>")[0]

    isins_in_page = re.findall(r'isin=(.*?)"',r)
    isins_list = isins_list + isins_in_page

    names_in_page = re.findall(r'isin=.*">(.*?)</a>', r)
    names_list = names_list + names_in_page

df = pd.DataFrame(list(zip(isins_list, names_list)), columns  = ["ISIN", "NAME"])
df["NAME"] = df["NAME"].str.replace(r'<wbr>', '')
df["NAME"] = df["NAME"].str.replace(r'&uuml;', 'u')
df.index += 1
df.to_excel("tradegate.xlsx")
print("Done")