

#functions
from _merge_utils import _pd_merge, _pd_merge_asof
from _zhao_functions import _irs_txt_to_dfs, _irs_contributors_screen, _irs_contributors_aggregate,\
    _echo_facilities_screen, _echo_enforcements_screen, _echo_milestones_screen, _echo_tri_screen, _echo_aggregate,\
    _violtrack_screen, _violtrack_aggregate,\
    _osha_inspection_screen, _osha_violation_screen, _osha_aggregate,\
    _crspcompustat_screen



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


#echo tri screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["TRI", "TRI_screen"]
#_echo_tri_screen(folders, items)


#merge echo facilities with enforcements
folders=["zhao/_epa"]
items=["CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen"]
left_path="zhao/_epa/CASE_FACILITIES_screen"
left_ons=["case_number", "activity_id",]
right_path="zhao/_epa/CASE_ENFORCEMENT_CONCLUSIONS_screen"
right_ons=["case_number", "activity_id"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge echo facilities_enforcements with milestones
folders=["zhao/_epa"]
items=["CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen"]
left_path="zhao/_epa/CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen"
left_ons=["case_number", "activity_id",]
right_path="zhao/_epa/CASE_MILESTONES_screen"
right_ons=["case_number", "activity_id"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge echo facilities_enforcements_milestones with tri, by nearest years
folders=["zhao/_epa"]
items=["CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen_TRI_screen"]
left_path="zhao/_epa/CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen"
left_bys=["registry_id"]
left_on="echo_initiation_year"
right_path="zhao/_epa/TRI_screen"
right_bys=["epa_registry_id"]
right_on="reporting_year"
#_pd_merge_asof(folders, items, left_path, left_bys, left_on, right_path, right_bys, right_on)


#violtrack screen
folders=["zhao/data/violation_tracker", "zhao/_violtrack"]
items=["ViolationTracker_basic_28jul23", "_violtrack_screen"]
#_violtrack_screen(folders, items)


#0
#osha violation screen
folders=["zhao/data/osha", "zhao/_osha"]
items=["osha_violation", "osha_violation_screen"]
#_osha_violation_screen(folders, items)


#0
#osha inspections screen
folders=["zhao/data/osha", "zhao/_osha"]
items=["osha_inspection", "osha_inspection_screen"]
#_osha_inspection_screen(folders, items)


#0
#merge osha violation with inspection
folders=["zhao/_osha"]
items=["osha_violation_screen_osha_inspection_screen"]
left_path="zhao/_osha/osha_violation_screen"
left_ons=["activity_nr"]
right_path="zhao/_osha/osha_inspection_screen"
right_ons=["activity_nr"]
how="inner"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#crspcompustat screen
folders=["zhao/data/crspcompustat", "zhao/_crspcompustat"]
items=["crspcompustat_2000_2023", "crspcompustat_2000_2023_screen"]
#_crspcompustat_screen(folders, items)


#search irs donations ids
folders=["zhao/_irs", "zhao/_irs"]
items=["A_screen", "A_screen_search"]
colname="a__company_involved"
#_search(folders, items, colname)


#search echo facilities_enforcements_milestones_tri ids
folders=["zhao/_epa", "zhao/_epa"]
items=["CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen_TRI_screen", "CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen_TRI_screen_search"]
colname="parent_co_name"
#_search(folders, items, colname)


#search violtrack ids
folders=["zhao/_violtrack", "zhao/_violtrack"]
items=["_violtrack_screen", "_violtrack_screen_search"]
colname="current_parent_name"
#_search(folders, items, colname)


#0
#search osha violation_inspection ids
folders=["zhao/_osha", "zhao/_osha"]
items=["osha_violation_screen_osha_inspection_screen", "osha_violation_screen_osha_inspection_screen_search"]
colname="estab_name"
#_search(folders, items, colname)



#merge irs donations with ids
folders=["zhao/_irs"]
items=["donations_ids"]
left_path="zhao/_irs/A_screen"
left_ons=["a__company_involved"]
right_path="zhao/_irs/A_screen_search_a__company_involved"
right_ons=["query"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge echo facilities_enforcements_tri with ids
folders=["zhao/_epa"]
items=["echo_ids"]
left_path="zhao/_epa/CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen_TRI_screen"
left_ons=["parent_co_name"]
right_path="zhao/_epa/CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen_TRI_screen_search_parent_co_name"
right_ons=["query"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge violtrack with ids
folders=["zhao/_violtrack"]
items=["violtrack_ids"]
left_path="zhao/_violtrack/_violtrack_screen"
left_ons=["current_parent_name"]
right_path="zhao/_violtrack/_violtrack_screen_search_current_parent_name"
right_ons=["query"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#0
#merge osha violation_inspection with ids
folders=["zhao/_osha"]
items=["osha_ids"]
left_path="zhao/_osha/osha_violation_screen_osha_inspection_screen"
left_ons=["estab_name"]
right_path="zhao/_osha/osha_violation_screen_osha_inspection_screen_search_estab_name"
right_ons=["query"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#irs donations aggregate
folders=["zhao/_irs", "zhao/_irs"]
items=["donations_ids", "donations_ids_aggregate"]
#_irs_contributors_aggregate(folders, items)


#violtrack aggregate
folders=["zhao/_violtrack", "zhao/_violtrack"]
items=["violtrack_ids", "violtrack_ids_aggregate"]
#_violtrack_aggregate(folders, items)


#echo aggregate
folders=["zhao/_epa", "zhao/_epa"]
items=["echo_ids", "echo_ids_aggregate"]
#_echo_aggregate(folders, items)


#0
#osha aggregate
folders=["zhao/_osha", "zhao/_osha"]
items=["osha_ids", "osha_ids_aggregate"]
#_osha_aggregate(folders, items)


#merge irs donations_ids_aggregate with echo_ids_aggregate
folders=["zhao/_merge"]
items=["donations_echo"]
left_path="zhao/_irs/donations_ids_aggregate"
left_ons=["cusip", "a__contribution_year"]
right_path="zhao/_epa/echo_ids_aggregate"
right_ons=["cusip", "echo_initiation_year"]
how="outer"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge irs donations_ids_aggregate with violtrack_ids_aggregate
folders=["zhao/_merge"]
items=["donations_violtrack"]
left_path="zhao/_irs/donations_ids_aggregate"
left_ons=["issueisin", "a__contribution_year"]
right_path="zhao/_violtrack/violtrack_ids_aggregate"
right_ons=["current_parent_isin", "violtrack_initiation_year"]
how="outer"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#0
#merge irs donations_ids_aggregate with osha_ids_aggregate
folders=["zhao/_merge"]
items=["donations_osha"]
left_path="zhao/_irs/donations_ids_aggregate"
left_ons=["cusip", "a__contribution_year"]
right_path="zhao/_epa/osha_ids_aggregate"
right_ons=["cusip", "osha_initiation_year"]
how="outer"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge irs donations_echo to crspcompustat
folders=["zhao/_merge"]
items=["donations_echo_crspcompustat"]
left_path="zhao/_merge/donations_echo"
left_ons=["cusip", "a__contribution_year"]
right_path="zhao/_crspcompustat/crspcompustat_2000_2023_screen"
right_ons=["cusip", "fyear"]
how="outer"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge irs donations_violtrack to crspcompustat
folders=["zhao/_merge"]
items=["donations_violtrack_crspcompustat"]
left_path="zhao/_merge/donations_violtrack"
left_ons=["cusip_left", "a__contribution_year"]
right_path="zhao/_crspcompustat/crspcompustat_2000_2023_screen"
right_ons=["cusip", "fyear"]
how="outer"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#0
#merge irs donations_osha to crspcompustat
folders=["zhao/_merge"]
items=["donations_osha_crspcompustat"]
left_path="zhao/_merge/donations_osha"
left_ons=["cusip", "a__contribution_year"]
right_path="zhao/_crspcompustat/crspcompustat_2000_2023_screen"
right_ons=["cusip", "fyear"]
how="outer"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#generate floats



print("done")


