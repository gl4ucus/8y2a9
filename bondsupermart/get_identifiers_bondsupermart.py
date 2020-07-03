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

browser.get("https://www.bondsupermart.com/bsm/bond-selector")

while True:
  try:
    load_more_button = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "/html/body/app-root/app-layout/div/app-bond-selector/div[2]/div/div[2]/nz-tabset/div[2]/div[1]/div[2]/button")))
    browser.execute_script("arguments[0].click();", load_more_button)
    
    print("Extracting in progress: " + str(len(set(re.findall(r'/bsm/bond-factsheet/(.*?)"', browser.page_source)))) + " Identifiers found")
  except:
    if ("Access Denied" in browser.page_source):
      sys.exit("Bondsupermart has blocked our IP temporarily. Please retry later.")
    else:
      Identifiers = re.findall(r'/bsm/bond-factsheet/(.*?)"', browser.page_source)
      Identifiers = list(set(Identifiers))
      print("A total of " + str(len(Identifiers)) + " unique identifier(s) has been extracted.")
      df = pd.DataFrame(Identifiers, columns = ["Identifiers"])
      df.to_csv(os.getcwd() + "/Identifier_records/bondsupermart_Identifiers " + str(date.today()) + ".csv", index = False)
      print("Data saved")
      break