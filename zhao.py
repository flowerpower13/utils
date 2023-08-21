

#functions
from _merge_utils import _pd_merge, _pd_merge_asof
from _zhao_functions import _irs_txt_to_dfs, \
    _irs_contributors_screen, _echo_facilities_screen, _echo_enforcements_screen, _echo_tri_screen, _osha_inspection_screen, _osha_violation_screen, _echo_milestones_screen, \
    _irs_contributors_aggregate, _violations_aggregate, _echo_aggregate, \
    _crspcompustat_dropdups


#irs from txt txt to dfs
folders=["zhao/data/irs", "zhao/_irs"]
items=["FullDataFile"]
#_irs_txt_to_dfs(folders, items)


#irs contributors screen
folders=["zhao/_irs", "zhao/_irs"]
items=["A", "A_screen"]
#_irs_contributors_screen(folders, items)


#echo facilities screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["CASE_FACILITIES", "CASE_FACILITIES_screen"]
#_echo_facilities_screen(folders, items)


#echo enforcements screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["CASE_ENFORCEMENT_CONCLUSIONS", "CASE_ENFORCEMENT_CONCLUSIONS_screen"]
#_echo_enforcements_screen(folders, items)


#echo milestones screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["CASE_MILESTONES", "CASE_MILESTONES_screen"]
#_echo_milestones_screen(folders, items)


#merge facilities with enforcements
folders=["zhao/_epa"]
items=["CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen"]
left_path="zhao/_epa/CASE_FACILITIES_screen"
left_ons=["case_number", "activity_id",]
right_path="zhao/_epa/CASE_ENFORCEMENT_CONCLUSIONS_screen"
right_ons=["case_number", "activity_id"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge facilities_enforcements with milestones
folders=["zhao/_epa"]
items=["CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen"]
left_path="zhao/_epa/CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen"
left_ons=["case_number", "activity_id",]
right_path="zhao/_epa/CASE_MILESTONES_screen"
right_ons=["case_number", "activity_id"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#echo tri screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["TRI", "TRI_screen"]
#_echo_tri_screen(folders, items)


#merge facilities_enforcements_milestones with tri, by nearest years
folders=["zhao/_epa"]
items=["CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen_TRI_screen"]
left_path="zhao/_epa/CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen"
left_bys=["registry_id"]
left_on="echo_initiation_year"
right_path="zhao/_epa/TRI_screen"
right_bys=["epa_registry_id"]
right_on="reporting_year"
#_pd_merge_asof(folders, items, left_path, left_bys, left_on, right_path, right_bys, right_on)


#osha violation screen
folders=["zhao/data/osha", "zhao/_osha"]
items=["osha_violation", "osha_violation_screen"]
#_osha_violation_screen(folders, items)


#osha inspections screen
folders=["zhao/data/osha", "zhao/_osha"]
items=["osha_inspection", "osha_inspection_screen"]
#_osha_inspection_screen(folders, items)


#merge violation with inspection
folders=["zhao/_osha"]
items=["osha_violation_screen_osha_inspection_screen"]
left_path="zhao/_osha/osha_violation_screen"
left_ons=["activity_nr"]
right_path="zhao/_osha/osha_inspection_screen"
right_ons=["activity_nr"]
how="inner"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#search donations ids
folders=["zhao/_irs", "zhao/_irs"]
items=["A_screen", "A_screen_search"]
colname="a__company_involved"
#_search(folders, items, colname)


#search facilities_enforcements_milestones_tri ids
folders=["zhao/_epa", "zhao/_epa"]
items=["CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen_TRI_screen", "CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen_TRI_screen_search"]
colname="parent_co_name"
#_search(folders, items, colname)


#search violation_inspection ids
folders=["zhao/_osha", "zhao/_osha"]
items=["osha_violation_screen_osha_inspection_screen", "osha_violation_screen_osha_inspection_screen_search"]
colname="estab_name"
#_search(folders, items, colname)


#merge donations with ids
folders=["zhao/_irs"]
items=["donations_ids"]
left_path="zhao/_irs/A_screen"
left_ons=["a__company_involved"]
right_path="zhao/_irs/A_screen_search_a__company_involved"
right_ons=["query"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge facilities_enforcements_tri with ids
folders=["zhao/_epa"]
items=["echo_ids"]
left_path="zhao/_epa/CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen_TRI_screen"
left_ons=["parent_co_name"]
right_path="zhao/_epa/CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen_TRI_screen_search_parent_co_name"
right_ons=["query"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge violation_inspection with ids
folders=["zhao/_osha"]
items=["osha_ids"]
left_path="zhao/_osha/osha_violation_screen_osha_inspection_screen"
left_ons=["estab_name"]
right_path="zhao/_osha/osha_violation_screen_osha_inspection_screen_search_estab_name"
right_ons=["query"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#donations aggregate
folders=["zhao/_irs", "zhao/_irs"]
items=["donations_ids", "donations_ids_aggregate"]
#_irs_contributors_aggregate(folders, items)


#0
#violations aggregate
folders=["zhao/data/violation_tracker", "zhao/_violations"]
items=["ViolationTracker_basic_28jul23", "violations_ids_aggregate"]
#_violations_aggregate(folders, items)


#echo aggregate
folders=["zhao/_epa", "zhao/_epa"]
items=["echo_ids", "echo_ids_aggregate"]
#_echo_aggregate(folders, items)


#osha aggregate
folders=["zhao/_osha", "zhao/_osha"]
items=["osha_ids", "osha_ids_aggregate"]
#_osha_aggregate(folders, items)


#0
#merge donations_ids_aggregate with violations_ids_aggregate
folders=["zhao/_merge"]
items=["donations_violations"]
left_path="zhao/_irs/donations_ids_aggregate"
left_ons=["issueisin", "a__contribution_year"]
right_path="zhao/_violations/violations_ids_aggregate"
right_ons=["current_parent_isin", "initiation_year_inferred_mean"]
how="outer"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge donations_ids_aggregate with echo_ids_aggregate
folders=["zhao/_merge"]
items=["donations_echo"]
left_path="zhao/_irs/donations_ids_aggregate"
left_ons=["cusip", "a__contribution_year"]
right_path="zhao/_epa/echo_ids_aggregate"
right_ons=["cusip", "echo_initiation_year"]
how="outer"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge donations_ids_aggregate with osha_ids_aggregate
folders=["zhao/_merge"]
items=["donations_osha"]
left_path="zhao/_irs/donations_ids_aggregate"
left_ons=["cusip", "a__contribution_year"]
right_path="zhao/_epa/osha_ids_aggregate"
right_ons=["cusip", "osha_initiation_year"]
how="outer"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#crspcompustat drop dups
folders=["zhao/data/crspcompustat", "zhao/_crspcompustat"]
items=["crspcompustat_2000_2023", "crspcompustat_2000_2023_dropdups"]
#_crspcompustat_dropdups(folders, items)


#0
#merge donations_violations to crspcompustat
folders=["zhao/_merge"]
items=["donations_violations_crspcompustat"]
left_path="zhao/_merge/donations_violations"
left_ons=["cusip", "a__contribution_year"]
right_path="zhao/_crspcompustat/crspcompustat_2000_2023_dropdups"
right_ons=["cusip", "fyear"]
how="outer"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge donations_echo to crspcompustat
folders=["zhao/_merge"]
items=["donations_echo_crspcompustat"]
left_path="zhao/_merge/donations_echo"
left_ons=["cusip", "a__contribution_year"]
right_path="zhao/_crspcompustat/crspcompustat_2000_2023_dropdups"
right_ons=["cusip", "fyear"]
how="outer"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge donations_osha to crspcompustat
folders=["zhao/_merge"]
items=["donations_osha_crspcompustat"]
left_path="zhao/_merge/donations_osha"
left_ons=["cusip", "a__contribution_year"]
right_path="zhao/_crspcompustat/crspcompustat_2000_2023_dropdups"
right_ons=["cusip", "fyear"]
how="outer"
validate="1:1"
_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)



#generate floats



print("done")


