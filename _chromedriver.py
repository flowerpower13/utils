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
search "Chrome/", see code (e.g., Chrome/103.0.5060.114)
 
DOWNLOAD PROPER CHROMEDRIVER
https://chromedriver.chromium.org/downloads
choose version close to current (e.g., 103)
"""


#SELENIUM
from selenium import webdriver
location="C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"
#set driver options
options=webdriver.ChromeOptions()
options.binary_location=(location)
#add arguments
args=[
    "--incognito", 
    "--enable-javascript",
    #"--headless", 
    "--disable-notifications",
    "--log-level=3",
    ]
for i, arg in enumerate(args):
    options.add_argument(arg)  


def _chromedriver(url):
    driver=webdriver.Chrome(executable_path="chromedriver", options=options)

    driver.get(url)
    text=driver.page_source

    driver.quit()

    return text

 