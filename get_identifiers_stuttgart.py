from selenium import webdriver
import time
import re
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')

browser = webdriver.Chrome("chromedriver", options=chrome_options)
browser.get('https://www.boerse-stuttgart.de/en/tools/finder-tools/bonds/')
browser.find_elements_by_xpath('/html/body/main/div[2]/div[1]/div/div/form/div/div[1]/div/div/button')[0].click()

while True: 
  try:
    # Try speed up with webdriver wait
    WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "/html/body/main/div[2]/div[2]/div/div/div[2]/button")))
    button = browser.find_element_by_xpath("/html/body/main/div[2]/div[2]/div/div/div[2]/button")
    browser.execute_script("arguments[0].click();", button)
    print(len(re.findall(r'idNotation=(.*?)"', browser.page_source)))
  except:
    break
  
id = re.findall(r'idNotation=(.*?)"', browser.page_source)
df = pd.DataFrame(id)
df.to_csv("stuttgart.csv", columns = ["Identifier"], index = False)
