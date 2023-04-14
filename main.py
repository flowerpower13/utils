
#functions
from _concat import _concat
from _pdfs_to_txts import _pdfs_to_txts
from _txts_to_counts import _txts_to_counts
from _filestem_to_ric import _filestem_to_ric
from _merge_utils import _pd_merge, _pd_concat
from _hassan import _txts_to_hassan, _txts_to_corpus, _topicbigrams


#import rdp
'''
from _rdp import _convert_symbols
import eikon as ek
import refinitiv.dataplatform as rdp
#right click on import "SymbolTypes", "Go to Definition"
from refinitiv.dataplatform.content.symbology.symbol_type import SymbolTypes
appkey="7203cad580454a948f17be1b595ef4884be257be"
ek.set_app_key(appkey)
rdp.open_desktop_session(appkey)
#'''


#######################################################################################################
#ADVEV
#COLLECT EARNINGS CALLS WITH REFINITIV WORKSPACE'S APP "ADVEV"
#search "advev"
#custom date range: 01/01/2001 to 02/01/2001 (do it month by month), select "show confirmed events only"
#event type: "earnings conference call"
#content type: "transcript"
#scroll down up to end, batch save button, "transcript", txt button 


#from pdf to txt
#create empty folders "_decrypt_pdf", "_raw_txt"
folders=["_advev", "_pdfs_to_txts"]
items=["_pdfs_to_txts"]
#_pdfs_to_txts(folders, items)


#from txt to score
folders=["_pdfs_to_txts", "_txts_to_counts"]
items=["_txts_to_counts"]
#bags and window size (count within n before, n after)
targetbag_keys=[
    "sovereign",
    ]
contextbag_keys=[
    "loughran_positive", 
    "loughran_negative", 
    "synonyms_uncertainty"
    ]
window_sizes=[10, 20]
#_txts_to_counts(folders, items, targetbag_keys, contextbag_keys, window_sizes)


#from pdf to txt
#create empty folders "_decrypt_pdf", "_raw_txt"
folders=["_traininglibraries_pdf", "_traininglibraries_txt"]
items=["_traininglibraries_txt"]
#_pdfs_to_txts(folders, items)


#training library's TF
#move libraries  from "_traininglibraries_txt" in correct folders either "libraries_p" or "libraries_p"
folders=["_traininglibraries_txt", "_topicbigrams"]
items={
    "p": ("libraries_p", "_topicbigrams_p", "_topicbigrams_pn"),
    "n": ("libraries_n", "_topicbigrams_n", "_topicbigrams_np"),
    }
_topicbigrams(folders, items)


#from txt to hassan-type score
#https://www.firmlevelrisk.com/
#https://github.com/mschwedeler/firmlevelrisk
folders=["_pdfs_to_txts", "_txts_to_hassan"]
items=["_txts_to_hassan"]
#_txts_to_hassan(folders, items)


#aggregate txt files
folders=["_txts_to_counts", "_concat"]
items=["_concat"]
full_db="_advev"
#_concat(folders, items, full_db)


#file_stem to ric
folders=["_concat", "_convert_symbols0"]
items=["_concat", "file_stem"]
#_filestem_to_ric(folders, items)
    

#from ric to isin
folders=["_convert_symbols0", "_convert_symbols1"]
items=["file_stem", "symbols"]
'''
IDs=[
    ["ric", SymbolTypes.RIC],
    ]
#'''
#_convert_symbols(folders, items, IDs)


#download compustat quarterly data
#https://wrds-www.wharton.upenn.edu/pages/get-data/compustat-capital-iq-standard-poors/compustat/global-daily/fundamentals-quarterly/
#2019-01 to 2021-12
#upload "_convert_symbols1/symbols_IssueISIN.txt", donwload all variables, .csv, .zip, submit to email
#save as "_data_compustat_isin.csv", put in folder "_data_compustat"

#download compustat quarterly data
#https://wrds-www.wharton.upenn.edu/pages/get-data/compustat-capital-iq-standard-poors/compustat/north-america-daily/fundamentals-quarterly/
#2019-01 to 2021-12
#upload "_convert_symbols1/symbols_CUSIP.txt", donwload all variables, .csv, .zip, submit to email
#save as "_data_compustat_cusip.csv", put in folder "_data_compustat"


#set e-calls database
folders=["_timeid_calls"]
items=["_timeid_calls0"]
left="_convert_symbols0/file_stem"
left_vars=["ric"]
right="_convert_symbols1/symbols"
right_vars=["ric"]
how="inner"
validate="m:1"
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


folders=["_timeid_calls"]
items=["_timeid_calls1"]
left="_concat/_concat"
left_vars=["file_stem"]
right="_timeid_calls/_timeid_calls0"
right_vars=["file_stem"]
how="inner"
validate="1:1"
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


#merge e-calls and compustat (isin)
folders=["_finaldb"]
items=["_finaldb_isin"]
left="_timeid_calls/_timeid_calls1"
left_vars=["year_quarter", "issueisin"]
right="_data_compustat/_data_compustat_isin"
right_vars=["datafqtr", "isin"]
how="inner"
validate="1:1"
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


#merge e-calls and compustat (cusip)
folders=["_finaldb"]
items=["_finaldb_cusip"]
left="_timeid_calls/_timeid_calls1"
left_vars=["year_quarter", "cusip"]
right="_data_compustat/_data_compustat_cusip"
right_vars=["datafqtr", "cusip"]
how="inner"
validate="1:1"
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


#merge isin and compustat
folders=["_finaldb"]
items=["_finaldb"]
left="_finaldb/_finaldb_isin"
right="_finaldb/_finaldb_cusip"
axis="index"
join="outer"
sort_id=["file_stem"]
#_pd_concat(folders, items, left, right, axis, join, sort_id)


'''
rdp.close_session()
print("_rdp - done")
#'''

print("done")