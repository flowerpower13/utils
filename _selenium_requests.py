#preliminary
"""
INSTALL BRAVE
https://brave.com/download/

LOCATE BRAVE APP
right click on Brave app, "Properties", "Open File Location"
/e.g., C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe)
set location in variable "location" (already done below)

CHECK BRAVE'S CHROME VERSION
open Brave, go to Address Bar, type "brave://version"
search "Chrome/", see code (e.g., Chrome/111.0.0.0)
 
DOWNLOAD PROPER CHROMEDRIVER
https://chromedriver.chromium.org/downloads
choose version close to current (e.g., 111)
"""


#import
import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
location="C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"


#chrome options
#https://github.com/SeleniumHQ/selenium/issues/10188
from selenium.webdriver.chrome.options import Options
chrome_options = Options()
chrome_options.binary_location=(location)
args=[
    "--incognito", 
    "--enable-javascript",
    #"--headless", 
    "--disable-notifications",
    "--log-level=3",
    ]
for i, arg in enumerate(args):
    chrome_options.add_argument(arg)  



def _chromedriver(url):
    driver=webdriver.Chrome(executable_path="chromedriver", options=chrome_options)

    driver.get(url)
    text=driver.page_source

    driver.quit()

    return text


#first instance
#https://stackoverflow.com/questions/8344776/can-selenium-interact-with-an-existing-browser-session
def _chromedriver_first(url):

    #launch driver
    driver = webdriver.Chrome(options=chrome_options)
    
    #get url_first and session id
    url_first = driver.command_executor._url
    session_id_first = driver.session_id

    #get url
    driver.get(url)

    return driver, url_first, session_id_first


#start from first instance
def _chromedriver_second(url_first, session_id_first, url):

    #launch driver
    driver=webdriver.Remote(command_executor=url_first, desired_capabilities={})
    
    #session id
    driver.close()
    driver.session_id = session_id_first
    
    #get url
    driver.get(url)
    text=driver.page_source

    return text



#requests text
'''
auth=("UnivBocc", "UbC4er53")
HEADERS={
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.6",
    "Cache-Control": "max-age=0",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    }
timeout=10
time_sleep=5
'''
def _requests_text(url, auth, HEADERS, timeout, time_sleep):

    #request
    r=requests.get(
        url, 
        auth=auth, 
        headers=HEADERS, 
        timeout=timeout, 
        stream=False, #use chunks
        )
    
    #time sleep
    time.sleep(time_sleep)

    #text
    text=r.text

    #close
    r.close()

    return text


#requests download
def _requests_dwn(url, output, auth, HEADERS, timeout, time_sleep):

    #request
    r=requests.get(
        url, 
        auth=auth, 
        headers=HEADERS, 
        timeout=timeout, 
        stream=True, #use chunks
        )

    #time sleep
    time.sleep(time_sleep)

    #save file
    chunk_size = 3000
    with open(output, 'wb') as fd:
        for chunk in r.iter_content(chunk_size):
            fd.write(chunk)

    #close
    r.close()

