

#copy to main.py
import eikon as ek
import refinitiv.dataplatform as rdp
import refinitiv.data as rd
from refinitiv.data.content import search
#right click on import "SymbolTypes", "Go to Definition"
from refinitiv.dataplatform.content.symbology.symbol_type import SymbolTypes
appkey="7203cad580454a948f17be1b595ef4884be257be"
ek.set_app_key(app_key=appkey)
rdp.open_desktop_session(app_key=appkey)
rd.open_session(app_key=appkey)


#functions
from _rdp import _search
from _merge_utils import _pd_merge
from _irs527 import _irstxt_to_dfs, _contributors_screen, _contributors_aggregate, _violations_aggregate


#from IRS txt to dfs
folders=["zhao/_donations", "zhao/_irstxt_to_dfs"]
items=["FullDataFile", "_irstxt_to_dfs"]
#_irstxt_to_dfs(folders, items)


#contributors screen by ein, keep companies name
folders=["zhao/_irstxt_to_dfs", "zhao/_contributors_screen"]
items=["A", "A_screen"]
#_contributors_screen(folders, items)


#search donations cusips
folders=["zhao/_contributors_screen", "zhao/_search"]
items=["A_screen", "A_search"]
colname="a__company_involved"
#_search(folders, items, colname)


#search violations cusips
folders=["zhao/_data/violation_tracker", "zhao/_search"]
items=["ViolationTracker_basic_28jul23", "ViolationTracker_basic_28jul23_search"]
colname="company"
_search(folders, items, colname)


#merge donations with cusip
folders=["zhao/_finaldb"]
items=["donations_cusip"]
left="zhao/_contributors_screen/A_screen"
left_vars=["a__company_involved"]
right="zhao/_search/A_search_A__company_involved"
right_vars=["query"]
how="left"
validate="m:1"
#'''
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


#merge violations with cusip
folders=["zhao/_finaldb"]
items=["violations_cusip"]
left="zhao/_data/violation_tracker/ViolationTracker_basic_28jul23"
left_vars=["company"]
right="zhao/_search/ViolationTracker_basic_28jul23_search_company"
right_vars=["query"]
how="left"
validate="m:1"
#'''
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


#aggregate donations
folders=["zhao/_finaldb", "zhao/_aggregate"]
items=["donations_cusip", "donations_cusip_aggregate"]
#_contributors_aggregate(folders, items)


#aggregate violations
folders=["zhao/_finaldb", "zhao/_aggregate"]
items=["violations_cusip", "violations_cusip_aggregate"]
#_violations_aggregate(folders, items)


#merge donations_cusip with violations_cusip
folders=["zhao/_finaldb"]
items=["donations_violations"]
left="zhao/_aggregate/donations_cusip_aggregate"
left_vars=["cusip", "year"]
right="zhao/_aggregate/violations_cusip_aggregate"
right_vars=["cusip", "year"]
how="outer"
validate="1:1"
#'''
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)



#Refinitiv data













#merge donations_cusip with compustat
folders=["zhao/_finaldb"]
items=["donations_cusip_compustat"]
left="zhao/_contributors_aggregate/A_aggregate"
left_vars=["cusip", "contribution_year"]
right="zhao/_data/crspcompustat_2000_2023"
right_vars=["cusip", "fyear"]
how="outer"
validate="1:1"
#'''
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)











rdp.close_session()
rd.close_session()



print("done")








#fuzzy match
#https://github.com/bradhackinen/nama


#Legis1
#https://legis1.com/


#L2
#https://l2-data.com/


#CONGRESSDATA APP
#https://cspp.ippsr.msu.edu/congress/


#LOBBYVIEW
#https://www.lobbyview.org/


#INTEGRITY WATCH
#https://data.integritywatch.eu/login


#PODCAST
# history of campaign finance disclosure laws (from FEC to IRS) 
# issue vs express advocacy
# create a pac https://www.fec.gov/help-candidates-and-committees/
# start a 527 https://www.irs.gov/charities-non-profits/political-organizations/user-guides-political-organizations-filing-and-disclosure-web-site
# start a 501   https://www.irs.gov/charities-non-profits/educational-resources-and-guidance-for-exempt-organizations
#               https://www.irs.gov/charities-non-profits/educational-resources-and-guidance-for-exempt-organizations
#               https://www.stayexempt.irs.gov/
# audit like IRS https://www.irs.gov/charities-non-profits/audit-technique-guides-atgs-and-technical-guides-tgs-for-exempt-organizations
# apply for R&D tax credit https://www.irs.gov/businesses/research-credit
# law updates for FEC, IRS, SEC, EPA, OSHA, USPTO, FDA https://www.federalregister.gov/agencies 


#IRS 527
#https://uscode.house.gov/ (Jump to: Title 26, 527)
#https://www.irs.gov/charities-non-profits/political-organizations
#https://www.irs.gov/charities-non-profits/political-organizations/political-organizations-resource-materials
#https://www.irs.gov/charities-non-profits/audit-technique-guides-atgs-and-technical-guides-tgs-for-exempt-organizations


#IRS 501
#https://uscode.house.gov/ (Jump to: Title 26, 501)
#https://www.irs.gov/charities-and-nonprofits
#https://www.irs.gov/charities-non-profits/required-filing-form-990-series (Go to schedule C)
#https://www.irs.gov/charities-non-profits/form-990-series-downloads
#https://www.irs.gov/statistics/soi-tax-stats-annual-extract-of-tax-exempt-organization-financial-data


#FEC
#https://www.fec.gov/help-candidates-and-committees/
#https://www.fec.gov/data/legal/statutes/


#CPA
#https://www.politicalaccountability.net/
#https://www.politicalaccountability.net/reports/cpa-reports/527data


#JUDGES PERSONAL DISCLOSURES
#https://pub.jefs.uscourts.gov/


#FMINUS
#https://fminus.org/database/


#VOTEVIEW
#https://voteview.com/data


#GOVINFO
#https://www.govinfo.gov/bulkdata


#CLERK
#https://disclosures-clerk.house.gov/


#US OGE
#https://www.oge.gov/web/oge.nsf/Officials%20Individual%20Disclosures%20Search%20Collection?OpenForm


#PROCUREMENT
#http://usaspending.gov/


#OPENSECRETS
#https://www.opensecrets.org/open-data/api-documentation
#https://github.com/opensecrets/python-crpapi


#PRO PUBLICA
#https://projects.propublica.org/api-docs/congress-api/
#https://projects.propublica.org/api-docs/campaign-finance/
#https://projects.propublica.org/nonprofits/api
#https://projects.propublica.org/free-the-files/api


#LDA
#https://lda.senate.gov/api/


#CONGRESS MEMBERS
#https://www.congress.gov/members


#VIOLATION TRACKER
#https://violationtracker.goodjobsfirst.org/?company_op=starts&company=&offense_group=&agency_code=OSHA
#MULTI, MN-AG, USAO


#CONGRESSMEN MISCONDUCT
#https://github.com/govtrack/misconduct/blob/master/misconduct-instances.csv






