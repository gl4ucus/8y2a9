import os
import sys
from datetime import date
from datetime import datetime
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

if (("bondsupermart_Identifiers " + str(date.today()) + ".csv") not in os.listdir(os.getcwd() + "/Identifier_records")):
  sys.exit("Today's identifiers have not been extracted. Exiting...")


# Identify the list of new Identifier that have been extracted today
if ("bondsupermart.csv" not in os.listdir(os.getcwd())):
  print("No previous list found.")
  new_Identifiers = pd.read_csv(os.getcwd() + "/Identifier_records/bondsupermart_Identifiers " + str(date.today()) + ".csv")["Identifier"].tolist()
else:
  bondsupermart = pd.read_csv("bondsupermart.csv")
  previous_Identifiers = bondsupermart["Identifier"].tolist()
  today_Identifiers = pd.read_csv(os.getcwd() + "/Identifier_records/bondsupermart_Identifiers " + str(date.today()) + ".csv")["Identifier"].tolist()
  new_Identifiers = list(np.setdiff1d(today_Identifiers, previous_Identifiers))
  
print("There is a total of " + str(len(new_Identifiers)) + " new identifier(s).")


# Scraping bond information for the list of new identifier

# Setting up the chrome driver
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
browser = webdriver.Chrome("chromedriver", options=chrome_options)

identifiers, isins, frequencys, senioritys, issuers, maturitys, currencys, ins_types, coupons, spreads = ([] for i in range(10))

for _ in new_Identifiers:
  try:
    browser.get("https://www.bondsupermart.com/bsm/bond-factsheet/" + str(_))

    isin = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/app-root/app-layout/div/bond-factsheet/span/div[2]/div/section[2]/div/nz-card/div/div/div[1]/div/div[3]/div"))).get_attribute('innerHTML')
    frequency = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/app-root/app-layout/div/bond-factsheet/span/div[2]/div/section[2]/div/nz-card/div/div/div[2]/div/div[5]"))).get_attribute('innerHTML')
    seniority = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/app-root/app-layout/div/bond-factsheet/span/div[2]/div/section[2]/div/nz-card/div/div/div[4]/div/div[4]"))).get_attribute('innerHTML')
    issuer = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/app-root/app-layout/div/bond-factsheet/span/div[2]/div/section[2]/div/nz-card/div/div/div[1]/div/div[1]"))).get_attribute('innerHTML')
    maturity = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/app-root/app-layout/div/bond-factsheet/span/div[2]/div/section[2]/div/nz-card/div/div/div[3]/div/div[4]"))).get_attribute('innerHTML')
    currency = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/app-root/app-layout/div/bond-factsheet/span/div[2]/div/section[2]/div/nz-card/div/div/div[2]/div/div[1]"))).get_attribute('innerHTML')
    ins_type = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/app-root/app-layout/div/bond-factsheet/span/div[2]/div/section[2]/div/nz-card/div/div/div[2]/div/div[1]"))).get_attribute('innerHTML')
    try:
      coupon = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/app-root/app-layout/div/bond-factsheet/span/div[2]/div/section[2]/div/nz-card/div/div/div[2]/div/div[2]"))).get_attribute('innerHTML')
    except:
      coupon = None
    
    try:
      spread = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/app-root/app-layout/div/bond-factsheet/span/div[2]/div/section[2]/div/nz-card/div/div/div[2]/div/div[4]/span"))).get_attribute('innerHTML')
    except:
      spread = None
    
    identifiers.append(_)
    isins.append(isin)
    frequencys.append(frequency)
    senioritys.append(seniority)
    issuers.append(issuer)
    maturitys.append(maturity)
    currencys.append(currency)
    ins_types.append(ins_type)
    coupons.append(coupon)
    spreads.append(spread)
    print(_, isin, frequency, seniority, issuer, maturity, currency, ins_type, coupon, spread)
  except:
    if ("Access Denied" in browser.page_source):
      sys.exit("Bondsupermart has blocked our IP temporarily. Please retry later.")
    continue

if ("bondsupermart.csv" not in os.listdir(os.getcwd())):
  old_df = pd.DataFrame()
else:
  old_df = pd.read_csv("bondsupermart.csv")

new_df = pd.DataFrame(list(zip(identifiers, isins, frequencys, senioritys, issuers, maturitys, currencys, ins_types, coupons, spreads)), columns = ["Identifier", "ISIN", "Frequency", "Seniority", "Issuer", "Maturity", "Currency", "Ins_type", "Coupon", "Spread"])
df = pd.concat([old_df, new_df], ignore_index = True)
df.to_csv("bondsupermart.csv", index = False)
print("Data saved")