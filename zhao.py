

#copy to main.py
'''
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
#'''


#functions
#from _rdp import _search
from _merge_utils import _pd_merge
from _irs527 import _irstxt_to_dfs, _contributors_screen, _contributors_aggregate, _violations_aggregate, _crspcompustat_removedups


#from IRS txt to dfs
folders=["zhao/_donations", "zhao/_irstxt_to_dfs"]
items=["FullDataFile", "_irstxt_to_dfs"]
#_irstxt_to_dfs(folders, items)


#contributors screen by ein, keep companies name
folders=["zhao/_irstxt_to_dfs", "zhao/_contributors_screen"]
items=["A", "A_screen"]
#_contributors_screen(folders, items)


#search donations ids
folders=["zhao/_contributors_screen", "zhao/_search"]
items=["A_screen", "A_search"]
colname="a__company_involved"
#_search(folders, items, colname)


#merge donations with ids
folders=["zhao/_finaldb"]
items=["donations_ids"]
left="zhao/_contributors_screen/A_screen"
left_vars=["a__company_involved"]
right="zhao/_search/A_search_A__company_involved"
right_vars=["query"]
how="left"
validate="m:1"
#'''
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


#aggregate donations (create var for each organization)
folders=["zhao/_finaldb", "zhao/_aggregate"]
items=["donations_ids", "donations_ids_aggregate"]
#_contributors_aggregate(folders, items)


#aggregate violations
folders=["zhao/_data/violation_tracker", "zhao/_aggregate"]
items=["ViolationTracker_basic_28jul23", "violations_ids_aggregate"]
#_violations_aggregate(folders, items)


#merge donations_ids_aggregate with violations_aggregate through isins
folders=["zhao/_finaldb"]
items=["donations_violations"]
left="zhao/_aggregate/donations_ids_aggregate"
left_vars=["issueisin", "a__contribution_year"]
right="zhao/_aggregate/violations_ids_aggregate"
right_vars=["current_parent_isin", "pen_year"]
how="outer"
validate="1:1"
#'''
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


#drop dups from crspcompustat
folders=["zhao/_data", "zhao/_data"]
items=["crspcompustat_2000_2023", "crspcompustat_2000_2023_withoutdups"]
#_crspcompustat_removedups(folders, items)


#merge donations_violations to crspcompustat
folders=["zhao/_finaldb"]
items=["donations_violations_crspcompustat"]
left="zhao/_finaldb/donations_violations"
left_vars=["cusip", "a__contribution_year"]
right="zhao/_data/crspcompustat_2000_2023_withoutdups"
right_vars=["cusip", "fyear"]
how="outer"
validate="1:1"
#'''
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


#des stats


#regs


'''
#copy to main.py
rdp.close_session()
rd.close_session()
#'''


print("done")



#Court Listener
#https://www.courtlistener.com/recap/
#https://www.courtlistener.com/help/api/rest/
#https://www.courtlistener.com/help/api/bulk-data/


#WRDS
#DOJ Cases














#fuzzy match
#https://github.com/bradhackinen/nama


#Free Law
#https://free.law/


#Federal Judicial Center Integrated Database
#https://www.fjc.gov/research/idb


#Legis1
#https://legis1.com/


#Judicial Watch
#JudicialWatch.org


#L2
#https://l2-data.com/


#CONGRESSDATA APP
#https://cspp.ippsr.msu.edu/congress/


#LOBBYVIEW
#https://www.lobbyview.org/


#EUINTEGRITY WATCH
#https://data.integritywatch.eu/login


#EU Court of Justice
#https://curia.europa.eu/jcms/jcms/j_6/en/


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

