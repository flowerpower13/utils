

#imports
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service


#vars
CHROME_DRIVER_PATH="chromedriver.exe"


#getSeleniumDriver
def getSeleniumDriver(CHROME_DRIVER_PATH):
    options = webdriver.ChromeOptions()
    options.add_argument("enable-automation")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--incognito")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--dns-prefetch-disable")
    options.add_argument("--disable-gpu")
    options.add_argument("--log-level=3")
    service = Service(executable_path=CHROME_DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    return driver


#isSignificantColumn
def isSignificantColumn(column, significantColumns):
    colClass = column["class"]
    for i in significantColumns:
        ninjaClass = f"ninja_column_{i}" 
        if ninjaClass in colClass:
            return True
    return False


#getTable
def getTable(driver, significantColumns, tableIndex):
    records = list()
    try:
        html = driver.find_elements(By.TAG_NAME, 'tbody')[tableIndex].get_attribute('innerHTML')
        soup = BeautifulSoup(html, "html.parser")

        tableRows = soup.findAll("tr")
        for row in tableRows:
            tableCols = row.findAll("td")
            record = list()
            for col in tableCols:
                if isSignificantColumn(col, significantColumns):
                    data = col.getText().strip()
                    record.append(data)
            records.append(record)
    except Exception as e:
        logging.error(e)
    return records


#isLastPage
def isLastPage(driver):
    isLastPage = True
    try:
        currentPageTag = driver.find_element(By.XPATH, '//li[@class="footable-page visible active"]')
        currentPage = int(currentPageTag.get_attribute('data-page'))
        nextPage = str(currentPage + 1)
        driver.find_element(By.XPATH, '//li[contains(@class, "footable-page")][@data-page="'+nextPage+'"]')
        isLastPage = False
    except:
        pass
    return isLastPage
        

#nextPage
def nextPage(driver):
    try:
        nextPageTag = driver.find_element(By.XPATH, '//a[@aria-label="next"]')
        nextPageTag.click()
        time.sleep(3)
    except:
        pass


#from url to df
def _url_to_df(url, colnames, filepath, significantColumns, tableIndex):
    driver = getSeleniumDriver()
    driver.get(url)
    time.sleep(10)

    records = list()
    records.append(colnames)
    pageNum = 1
    while True:
        logging.info(f"Fetching data from page {pageNum}")
        records = records + getTable(driver, significantColumns, tableIndex)
        if isLastPage(driver):
            break
        nextPage(driver)
        pageNum += 1
    
    logging.info("Saving data...")
    df = pd.DataFrame(records)
    df.to_csv(
        filepath,
        index=False,
        header=None,
        quotechar='"',
        )
    
    logging.info("Done!")

    
#settlements
#url
url="https://attorneysgeneral.org/settlements-and-enforcement-actions/searchable-list-of-settlements-1980-present/"
#column names
colnames=[
    "Date of Settlement",
    "Settling Entities",
    "Issue Area (General)",
    "Issue Area (Specific)",
    "Number of Participating AGs",
    "Federal Involvement",
    "Total Settlement Amount",
    "Total Share to States",
    "Total Consumer Restitution",
    ]
#ninja columns
significantColumns=[3, 4, 5, 6, 7, 8, 9, 10, 11]
tableIndex=0
#filepath
filepath="zhao/_litigation/settlements.csv"
logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
#_url_to_df(url, colnames, filepath, significantColumns, tableIndex)


#lawsuits against govt
#url
url="https://attorneysgeneral.org/list-of-lawsuits-1980-present/"
#column names
colnames=[
    "Case Caption",
    "Date AGs Initiated",
    "Date Case Resolved",
    "Apecific Policy",
    "# of Suing AGs",
    "Docket or Citation",
    "Original Court",
    "Current or Final Court",
    "Current Status or Resolution",
    ]
#start and end ninja columns
significantColumns=[2, 5, 6, 9, 10, 11, 13, 18, 29]
tableIndex=3
#filepath
filepath="zhao/_litigation/lawsuits.csv"
logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
#_url_to_df(url, colnames, filepath, significantColumns, tableIndex)





