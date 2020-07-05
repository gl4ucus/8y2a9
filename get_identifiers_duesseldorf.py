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

browser.get("https://www.boerse-duesseldorf.de/anleihen/detailsuche?page=1&emittent_id%7Blookup%7D=&emittent_land_id=&emittent_id=&kupon_typ_id=&waehrung_id=&min_kupon%7Bprice%7D=&max_kupon%7Bprice%7D=&min_restlaufzeit=&max_restlaufzeit=&min_rendite=&max_rendite=&pari=&do=1")

last_page_url = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/div[1]/div[5]/div[3]/div/div[1]/div/div/form/div/div/div[2]/div[2]/div/a[9]"))).get_attribute('href')

# Visting from first page until last page, and collecting identifiers along the way.
Identifiers, Issuers, Currencys, Maturitys, Coupons  = ([] for i in range(5))
page = 1
while True:
  url = "https://www.boerse-duesseldorf.de/anleihen/detailsuche?page=" + str(page) + "&emittent_id%7Blookup%7D=&emittent_land_id=&emittent_id=&kupon_typ_id=&waehrung_id=&min_kupon%7Bprice%7D=&max_kupon%7Bprice%7D=&min_restlaufzeit=&max_restlaufzeit=&min_rendite=&max_rendite=&pari=&do=1"
  browser.get(url)
  Identifiers.extend([i.text for i in browser.find_elements_by_xpath('//*[@class="ar_standard_table"]//tbody//tr//td["first"]//span[@class = "wkn"]//a')])
  Issuers.extend([i.text for i in browser.find_elements_by_xpath('//*[@class="ar_standard_table"]//tbody//tr//td["first"]//span[@class = "emittent_name"]')])
  Currencys.extend([i.text for i in browser.find_elements_by_xpath('//*[@class="ar_standard_table"]//tbody//tr//td["first"]//span[@class = "currency"]')])
  Maturitys.extend([i.text for i in browser.find_elements_by_xpath('//*[@class="ar_standard_table"]//tbody//tr//td["first"]//span[@class = "faelligkeit"]')])
  Coupons.extend([i.text for i in browser.find_elements_by_xpath('//*[@class="ar_standard_table"]//tbody//tr//td["first"]//span[@class = "kupon"]')])
  print("Extracting in progress: " + str(len(Identifiers)) + " Identifiers found")
  page = page + 1
  if url == last_page_url:
    break

df = pd.DataFrame(list(zip(Identifiers, Issuers, Currencys, Maturitys, Coupon)), columns = ["Identifier", "Issuer", "Currency", "Maturity", "Coupon"])
