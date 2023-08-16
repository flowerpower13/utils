

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
from _zhao_functions import _irstxt_to_dfs, _contributors_screen, _contributors_aggregate, \
    _violations_aggregate, _crspcompustat_dropdups, _echo_screen, _tri_screen, _echo_tri_aggregate


#from IRS txt to dfs
folders=["zhao/data/irs", "zhao/_irs"]
items=["FullDataFile"]
#_irstxt_to_dfs(folders, items)


#contributors screen by ein, keep companies name
folders=["zhao/_irs", "zhao/_irs"]
items=["A", "A_screen"]
#_contributors_screen(folders, items)


#echo screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["ECHO_EXPORTER", "ECHO_EXPORTER_screen"]
#_echo_screen(folders, items)


#tri screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["tri", "tri_screen"]
#_tri_screen(folders, items)


#merge echo with tri
folders=["zhao/_epa"]
items=["ECHO_EXPORTER_screen_tri_screen"]
left="zhao/_epa/ECHO_EXPORTER_screen"
left_vars=["registry_id"]
right="zhao/_epa/tri_screen"
right_vars=["epa_registry_id"]
how="left"
validate="1:1"
#'''
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


#search donations ids
folders=["zhao/_irs", "zhao/_irs"]
items=["A_screen", "A_screen_search"]
colname="a__company_involved"
#_search(folders, items, colname)


#search echo_tri ids
folders=["zhao/_epa", "zhao/_epa"]
items=["ECHO_EXPORTER_screen_tri_screen", "ECHO_EXPORTER_screen_tri_screen_search"]
colname="parent_co_name"
#_search(folders, items, colname)


#merge donations with ids
folders=["zhao/_irs"]
items=["donations_ids"]
left="zhao/_irs/A_screen"
left_vars=["a__company_involved"]
right="zhao/_irs/A_screen_search_a__company_involved"
right_vars=["query"]
how="left"
validate="m:1"
#'''
_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


#merge echo_tri with ids
folders=["zhao/_epa"]
items=["echo_tri_ids"]
left="zhao/_epa/ECHO_EXPORTER_screen_tri_screen"
left_vars=["parent_co_name"]
right="zhao/_epa/ECHO_EXPORTER_screen_tri_screen_search_parent_co_name"
right_vars=["query"]
how="left"
validate="m:1"
#'''
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


#aggregate donations
folders=["zhao/_irs", "zhao/_irs"]
items=["donations_ids", "donations_ids_aggregate"]
#_contributors_aggregate(folders, items)


#0
#aggregate violations
folders=["zhao/data/violation_tracker", "zhao/_violations"]
items=["ViolationTracker_basic_28jul23", "violations_ids_aggregate"]
#_violations_aggregate(folders, items)


#aggregate echo_tri
folders=["zhao/_epa", "zhao/_epa"]
items=["echo_tri_ids", "echo_tri_ids_aggregate"]
#_echo_tri_aggregate(folders, items)


#0
#merge donations_ids_aggregate with violations_ids_aggregate
folders=["zhao/_merge"]
items=["donations_violations"]
left="zhao/_irs/donations_ids_aggregate"
left_vars=["issueisin", "a__contribution_year"]
right="zhao/_violations/violations_ids_aggregate"
right_vars=["current_parent_isin", "initiation_year_inferred_mean"]
how="outer"
validate="1:1"
#'''
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


#merge donations_ids_aggregate with echo_tri_ids_aggregate
folders=["zhao/_merge"]
items=["donations_echo_tri"]
left="zhao/_irs/donations_ids_aggregate"
left_vars=["issueisin", "a__contribution_year"]
right="zhao/_epa/echo_tri_ids_aggregate"
right_vars=["issueisin", "echotri_initiation_year"]
how="outer"
validate="1:1"
#'''
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


#drop dups from crspcompustat
folders=["zhao/data/crspcompustat", "zhao/_crspcompustat"]
items=["crspcompustat_2000_2023", "crspcompustat_2000_2023_dropdups"]
#_crspcompustat_dropdups(folders, items)


#0
#merge donations_violations to crspcompustat
folders=["zhao/_merge"]
items=["donations_violations_crspcompustat"]
left="zhao/_merge/donations_violations"
left_vars=["cusip", "a__contribution_year"]
right="zhao/_crspcompustat/crspcompustat_2000_2023_dropdups"
right_vars=["cusip", "fyear"]
how="outer"
validate="1:1"
#'''
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


#merge donations_echo_tri to crspcompustat
folders=["zhao/_merge"]
items=["donations_echo_tri_crspcompustat"]
left="zhao/_merge/donations_echo_tri"
left_vars=["cusip_left", "a__contribution_year"]
right="zhao/_crspcompustat/crspcompustat_2000_2023_dropdups"
right_vars=["cusip", "fyear"]
how="outer"
validate="1:1"
#'''
_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


#generate floats








'''
#copy to main.py
rdp.close_session()
rd.close_session()
#'''


print("done")


