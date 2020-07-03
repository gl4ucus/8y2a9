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

if (("berlin_Identifiers " + str(date.today()) + ".csv") not in os.listdir(os.getcwd() + "/Identifier_records")):
  sys.exit("Today's identifiers have not been extracted. Exiting...")


# Identify the list of new Identifier that have been extracted today
if ("berlin.csv" not in os.listdir(os.getcwd())):
  print("No previous list found.")
  new_Identifiers = pd.read_csv(os.getcwd() + "/Identifier_records/berlin_Identifiers " + str(date.today()) + ".csv")["Identifier"].tolist()
else:
  berlin = pd.read_csv("berlin.csv")
  previous_Identifiers = berlin["Identifier"].tolist()
  today_Identifiers = pd.read_csv(os.getcwd() + "/Identifier_records/berlin_Identifiers " + str(date.today()) + ".csv")["Identifier"].tolist()
  new_Identifiers = list(np.setdiff1d(today_Identifiers, previous_Identifiers))
  
print("There is a total of " + str(len(new_Identifiers)) + " new identifier(s).")


# Scraping bond information for the list of new identifier

# Setting up the chrome driver
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
browser = webdriver.Chrome("chromedriver", options=chrome_options)

identifiers, names, isins, issuers, currencys, frequencys, ins_types, last_trading_days= ([] for i in range(8))

for _ in new_Identifiers:
  try:
    browser.get("https://www.boerse-berlin.com/index.php/Bonds?isin=" + str(_))

    name = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/div/div[3]/div/div/div[1]/div[3]/h1"))).get_attribute('innerHTML')
    isin = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/div/div[3]/div/div/div[2]/div[1]/div[1]/div[1]/div[2]/div[2]/div[2]/div[2]/span"))).get_attribute('innerHTML')
    issuer = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/div/div[3]/div/div/div[2]/div[1]/div[1]/div[1]/div[2]/div[2]/div[3]/div[2]/span"))).get_attribute('innerHTML')
    currency = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/div/div[3]/div/div/div[2]/div[1]/div[1]/div[1]/div[2]/div[2]/div[8]/div[2]/span"))).get_attribute('innerHTML')
    frequency = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/div/div[3]/div/div/div[2]/div[1]/div[1]/div[1]/div[2]/div[2]/div[9]/div[2]/span"))).get_attribute('innerHTML')
    ins_type = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/div/div[3]/div/div/div[2]/div[1]/div[1]/div[1]/div[2]/div[2]/div[10]/div[2]/span"))).get_attribute('innerHTML')
    last_trading_day = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/div/div[3]/div/div/div[2]/div[1]/div[1]/div[1]/div[2]/div[2]/div[11]/div[2]/span"))).get_attribute('innerHTML')
 
    identifiers.append(_)
    names.append(name)
    isins.append(isin)
    issuers.append(issuer)
    currencys.append(currency)
    frequencys.append(frequency)
    ins_types.append(ins_type)
    last_trading_days.append(last_trading_day)
    
    print(_, isin, name, issuer, currency, frequency, ins_type, last_trading_day)
  except:
    print(_ + " is skipped.")
    continue

if ("berlin.csv" not in os.listdir(os.getcwd())):
  old_df = pd.DataFrame()
else:
  old_df = pd.read_csv("berlin.csv")

new_df = pd.DataFrame(list(zip(identifiers, isins, names, issuers, currencys, frequencys, ins_types, last_trading_days)), columns = ["Identifier", "ISIN", "Name", "Issuer", "Currency", "Frequency", "Ins_Type", "Last_Trading_Day"])
df = pd.concat([old_df, new_df], ignore_index = True)
df.drop_duplicates(subset = "Identifier", inplace = True, keep = 'last')
df.to_csv("berlin.csv", index = False)
print("Data saved")
print(df.shape)