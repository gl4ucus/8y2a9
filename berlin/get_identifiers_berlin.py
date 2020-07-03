import os
import re
import sys
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import date

# Setting up the chrome driver
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
browser = webdriver.Chrome("chromedriver", options=chrome_options)

browser.get("https://www.boerse-berlin.com/index.php/Bonds/Search?table_pi=1&search[submit]=Suche&rowsperpage=500")

last_page_url = browser.find_element_by_xpath("/html/body/div/div[3]/div/div/div[2]/div[1]/div[1]/div[3]/div[1]/a[7]").get_attribute('href')

# Visting from first page until last page, and collecting identifiers along the way.
Identifiers = []
table_pi = 1
while True:
  url = "https://www.boerse-berlin.com/index.php/Bonds/Search?table_pi=" + str(table_pi) + "&search[submit]=Suche&rowsperpage=500"
  browser.get(url)
  Identifiers.extend(re.findall(r'isin=(.*?)">', browser.page_source))
  print("Extracting in progress: " + str(len(set(Identifiers))) + " Identifiers found")
  table_pi = table_pi + 1
  if url == last_page_url:
    break

Identifiers = list(set(Identifiers))
print("A total of " + str(len(Identifiers)) + " unique identifier(s) has been extracted.")
df = pd.DataFrame(Identifiers, columns = ["Identifier"])
df.to_csv(os.getcwd() + "/Identifier_records/berlin_Identifiers " + str(date.today()) + ".csv", index = False)
print("Data saved")