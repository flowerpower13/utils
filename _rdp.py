

import io
import re
import json
import time
import openpyxl
import numpy as np
import pandas as pd
from pathlib import Path
from openpyxl.utils.dataframe import dataframe_to_rows


#functions
from _pd_utils import _clean_stem, _df_to_csvcols, _dfcol_to_listcol
from _standardize_names import _standardize_query


#copy to main.py
#'''
import eikon as ek
import refinitiv.dataplatform as rdp
import refinitiv.data as rd
from refinitiv.data.content import search
#right click on import "SymbolTypes", "Go to Definition"
from refinitiv.dataplatform.content.symbology.symbol_type import SymbolTypes
appkey="7203cad580454a948f17be1b595ef4884be257be"
#appkey="50c941d00efb4106aab098286e2fd1ff3d463c67"
ek.set_app_key(app_key=appkey)
rdp.open_desktop_session(app_key=appkey)
rd.open_session(app_key=appkey)
#'''


#retrieve n obs
def _n_obs(IDs, df):

    #n obs
    n_obs=0

    #for
    for j, ID in enumerate(IDs):
        col_name=ID[0]

        symbols_all=_dfcol_to_listcol(df, col_name)

        n_obs+=len(symbols_all)

    #return
    return n_obs


#divide in chunks
def chunker(seq, size):

    #chunks
    chunks=(seq[pos:pos + size] for pos in range(0, len(seq), size))

    #chunks list
    chunks_list=list(chunks)

    #return
    return chunks_list


#from IDs to new symbols df
def _convert_IDs(df, IDs):

    #n chunks
    n_chunks=500

    #new symbols to find
    to_symbol_types=[e for e in SymbolTypes]
    enum_values=[e.value for e in SymbolTypes]

    #n obs
    n_obs=_n_obs(IDs, df)
    frames=[None]*n_obs

    #init i
    i=0

    #for
    for j, ID in enumerate(IDs):

        #identifiers
        col_name=ID[0]
        from_symbol_type=ID[1]

        #retrieve symbols list
        symbols_all=_dfcol_to_listcol(df, col_name)

        #divide in chucks
        chunks_list=chunker(symbols_all, n_chunks)

        #for
        for k, symbols in enumerate(chunks_list):

            #try
            try:

                #convert symbols
                df_i=rdp.convert_symbols(
                    symbols=symbols, 
                    from_symbol_type=from_symbol_type, 
                    to_symbol_types=to_symbol_types,
                    )
                    
            #except
            except Exception as e:
                print(e)

            #re index
            df_i=df_i.reindex(columns=enum_values)
            df_i=df_i.reindex(index=symbols_all)  

            #drop nulls and duplicates
            df_i=df_i.dropna(subset=["RIC"]) 
            df_i=df_i.drop_duplicates(subset=["RIC"]) 

            #franes
            frames[i]=df_i

            #update i
            i+=1

    #frames
    df=pd.concat(frames)

    #rename index
    df=df.rename_axis("idx")

    #return
    return df


#CONVERT SYMBOLS
folders=["zhao/_crspcompustat", "zhao/_crspcompustat"]
items=["crspcompustat_2000_2023_screen", "crspcompustat_2000_2023_screen_convert"]
IDs=[
    ["cusip", SymbolTypes.CUSIP],
    ]
#right click on import "SymbolTypes", "Go to Definition"
def _convert_symbols(folders, items, IDs):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        )

    #from IDs to new symbols df
    df=_convert_IDs(df, IDs)

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath)

    #for each column, create a csv
    _df_to_csvcols(df, results, result)


#load items
def _load_items(resources, item, col_symbol):

    #index
    idx=item.rindex("_")

    #col names
    colname=item[idx+1:]
    colnames=["fields", "parameters", "countries"]

    #if
    if colname in colnames:
        pass

    #elif
    elif not (colname in colnames):
        colname=col_symbol

    #read
    filepath=f"{resources}/{item}.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        )
    
    #df list
    df_list=df[colname].to_list()

    #return
    return df_list


def _print_msg(msg, j, tot_j, k, tot_k, jay, kay):

    #print
    print(f"j: {j}/{tot_j} - k: {k}/{tot_k} - {jay}_{kay} - {msg}")


#screener loop
def _screener_loop(jays, kays, results):

    #https://developers.refinitiv.com/en/article-catalog/article/find-your-right-companies-with-screener-eikon-data-apis-python

    #jays=jays[0:2]
    #kays=kays[0:5]

    n_obs_j=len(jays)
    tot_j=n_obs_j-1
    frames_j=[None]*n_obs_j

    n_obs_k=len(kays)
    tot_k=n_obs_k-1
    frames_k=[None]*n_obs_k

    idx="Instrument"

    for j, jay in enumerate(jays):

        for k, kay in enumerate(kays):

            filestem=f"{jay}_{kay}"
            filestem=_clean_stem(filestem)
            output=Path(f"{results}/{filestem}.csv")

            if output.is_file():
                df_k=pd.read_csv(output, dtype="string")
                df_k=df_k.drop_duplicates(subset=[idx])
                df_k=df_k.set_index(idx)

                msg="already done"
                _print_msg(msg, j, tot_j, k, tot_k, jay, kay)
                

            elif not output.is_file():

                instr=f'SCREEN(U(IN(DEALS)/*UNV:DEALSMNA*/), IN(TR.MnANationHQ,"{jay}"), CURN=USD)'
                try:
                    df_k, err=ek.get_data(
                        instruments=instr, 
                        fields=kay,
                        parameters=None, 
                        field_name=True,
                        raw_output=False,
                        debug=False,
                        )

                    if err is not None:
                        err=err[0]
                        df_k=pd.DataFrame()

                        msg="error"
                        _print_msg(msg, j, tot_j, k, tot_k, jay, kay)
                        print(err)

                    elif err is None:
                        df_k=df_k.drop_duplicates(subset=[idx])
                        df_k=df_k.set_index(idx)
                        df_k.insert(0, "jay", jay)
                        df_k.insert(1, "kay", kay)
                        df_k.to_csv(output)

                        msg="done"
                        _print_msg(msg, j, tot_j, k, tot_k, jay, kay)
                        #time.sleep(1/5)

                except Exception as e:
                    df_k=pd.DataFrame()

                    msg="exception"
                    _print_msg(msg, j, tot_j, k, tot_k, jay, kay)
                    print(e)

            frames_k[k]=df_k

        df_j=pd.concat(frames_k, axis="columns")
        #df_j=df_j[~df_j.index.duplicated(keep='first')]
        frames_j[j]=df_j

    df=pd.concat(frames_j, axis="index")
    df=df.dropna(axis="columns", how="all")
    df=df.loc[:,~df.columns.duplicated()]

    filepath=f"{results}/{results}.csv"
    df.to_csv(filepath, index_label=idx)


#get data loop
def _get_data_loop(instruments, fields, parameters, results):

    #n obs
    n_obs=len(instruments)*len(parameters)
    tot=n_obs-1

    #init lists
    frames=[None]*n_obs
    filestems=[None]*n_obs
    converteds=[None]*n_obs

    #init i
    i=0

    #for
    for j, instr in enumerate(instruments):
        print(j, instr)

        #for
        for k, param in enumerate(parameters):
            
            #filestem
            filestem=f"{instr}_{param}"
            filestem=_clean_stem(filestem)

            #output
            output=Path(f"{results}/{filestem}.csv")

            #if
            if output.is_file():

                #read
                df=pd.read_csv(
                    output,
                    dtype="string",
                    )
                
                #set index
                df=df.set_index("filestem")

                #converted
                converted=True

                #print
                print(f"{i}/{tot} - {filestem} - already done")
                
            #elif
            elif not output.is_file():

                #if
                if pd.isna(instr):

                    #df
                    df=pd.DataFrame()

                    #converted
                    converted=False

                    #print
                    print(f"{i}/{tot} - {filestem} - already missing in original db")  

                #elif
                elif not pd.isna(instr):

                    #param                    
                    param=json.loads(param)
                    
                    #try
                    try:

                        #get data
                        df, err=ek.get_data(
                            instruments=instr, 
                            fields=fields, 
                            parameters=param, 
                            field_name=True,
                            raw_output=False,
                            debug=False,
                            )
                        
                        #if
                        if err is not None:

                            #error
                            err=err[0]

                            #df
                            df=pd.DataFrame()

                            #converted
                            converted=False

                            #print
                            print(f"{i}/{tot} - {filestem} - error")
                            print(err)
                        
                        #elif
                        elif err is None:

                            #if
                            if df is None:

                                #df
                                df=pd.DataFrame()

                                #converted
                                converted=False

                                #print
                                print(f"{i}/{tot} - {filestem} - df none")
                            
                            #elif
                            elif df is not None:   
                                
                                #if
                                if df.empty==True:

                                    #df
                                    df=pd.DataFrame()

                                    #converted
                                    converted=False

                                    #print
                                    print(f"{i}/{tot} - {filestem} - df empty")
                                
                                #elif
                                elif df.empty==False:
                                    df.insert(0, "filestem", filestem)
                                    df=df.set_index("filestem")
                                    df.to_csv(output)

                                    #converted
                                    converted=True

                                    #print
                                    print(f"{i}/{tot} - {filestem} - done")

                                    #sleep
                                    time.sleep(1/5)

                    #except
                    except Exception as e:

                        #df
                        df=pd.DataFrame()

                        #converted
                        converted=False
                        
                        #print
                        print(f"{i}/{tot} - {filestem} - exception")
                        print(e)

            #update frames
            frames[i]=df
            filestems[i]=filestem
            converteds[i]=converted

            #update i
            i+=1
            
    #concat
    df=pd.concat(frames)

    #reindex
    df=df.reindex(index=filestems)

    #insert
    df.insert(1, "converted", converteds)  

    #save
    filepath=f"{results}/{results}.csv"
    df.to_csv(filepath)


#GET DATA
folders=["zhao/_get_data0", "zhao/_get_data1"]
items=["filestem_idx", "filestem_fields", "filestem_parameters"]
symbol=["OAPermID", "MULTI"]
def _get_data(folders, items, symbol):

    #folders
    resources=folders[0]
    results=folders[1]

    #symbols
    col_symbol=symbol[0]
    code_symbol=symbol[1]

    #idx
    item=items[0]
    instruments=_load_items(resources, item, col_symbol)

    #fields
    item=items[1]
    fields=_load_items(resources, item, col_symbol)

    #parameters
    item=items[2]
    parameters=_load_items(resources, item, col_symbol)
    
    #countries
    #item=items[3]
    #countries=_load_items(resources, item, col_symbol)  

    #screener - choose!
    #s_screener_loop(countries, fields, results)

    #get data loop - choose!
    _get_data_loop(instruments, fields, parameters, results)


#extract year
def _extract_year(x):

    #data_dict
    data_dict=json.loads(x)

    #year
    year=data_dict["Period"]

    #return
    return year


#RDP DATA FROM EXCEL
folders=["zhao/_crspcompustat", "zhao/_rdp_data1"]
items=["crspcompustat_2000_2023_screen_convert", "filestem_fields", "filestem_parameters"]
symbol=["RIC", "RIC"]
#symbol=["IssueISIN", "ISIN"]
#symbol=["OAPermID", "MULTI"]
def _rdp_data1(folders, items, symbol):

    #folders
    resources=folders[0]
    results=folders[1]

    #symbols
    col_symbol=symbol[0]
    code_symbol=symbol[1]

    #instruments_df
    item=items[0]
    filepath=f"{resources}/{item}.csv"
    instruments_df=pd.read_csv(
        filepath,
        dtype="string",
        )
    symbols_all=_dfcol_to_listcol(instruments_df, col_symbol)

    #fields_df
    item=items[1]
    filepath=f"{resources}/{item}.csv"
    fields_df=pd.read_csv(
        filepath,
        dtype="string",
        )

    #parameters
    item=items[2]
    parameters=_load_items(resources, item, col_symbol)
    parameters=[_extract_year(x) for x in parameters]

    #optional
    optional="CH=Fd RH=IN"

    #code
    code=f'CODE={code_symbol}'

    #chunks
    n_chunks=7000
    chunks_list=chunker(symbols_all, n_chunks)

    #for
    for j, param in enumerate(parameters):

        #for
        for k, chunk in enumerate(chunks_list):

            #wb
            wb=openpyxl.Workbook()

            #instruments chunk
            d={
                col_symbol: chunk,
                }
            instruments_chunk=pd.DataFrame(data=d) 

            #sheets
            sheets={
                "instruments": instruments_chunk, 
                "fields": fields_df,
                }

            #for
            for title, df in sheets.items():

                #ws
                ws=wb.create_sheet(title=title)
                rows=dataframe_to_rows(df, index=True, header=True)

                #for
                for l, row in enumerate(rows):
                    ws.append(row)

            #rdp data
            rdp_data=f'=@RDP.Data(instruments!B3:B10000,fields!B3:B100,"Period={param} {optional} {code}")'

            #ws
            ws_data=wb.active
            ws_data.title=param
            ws_data["A1"]=rdp_data
            
            #save
            filepath=f"{results}/{param}_{k}.xlsx"
            wb.save(filepath)

            #print
            print(f"{param}_{k} - done")


#_rdp_data1(folders, items, symbol)


#CONCATATENATE EXCEL SHEETS FOR RDP DATA
folders=["zhao/_rdp_data1", "zhao/_rdp_data2"]
def _rdp_data2(folders):
    resources=folders[0]
    results=folders[1]

    p=Path(resources).glob('**/*')
    files=[x for x in p if x.is_file()]

    n_obs=len(files)
    tot=n_obs-1
    frames=[None]*n_obs

    for i, file in enumerate(files):
        filepath=f"{file}"
        df=pd.read_excel(
            filepath,
            #engine="openpyxl",
            )

        #rename instruments
        first_col=df.columns[0]
        df=df.rename(columns={first_col: "RIC"})

        filestem=file.stem
        df.insert(0, "period", filestem)

        frames[i]=df

        print(f"{i}/{tot} - {file} - done")

    df=pd.concat(frames)

    filepath=f"{results}/summary.csv"
    df.to_csv(filepath, index=False)
    

#search loop
def _search_loop(view, query, filter, select, top, i, tot):

    #search
    df=rd.discovery.search(
        view=view,
        query=query,
        filter=filter,
        select=select,
        top=top,
        )
    
    #not empty
    if not df.empty:
    
        #converted
        converted=True

        #sleep
        time.sleep(1/5)

        #print
        print(f"{i}/{tot} - {query} - done")
    
    #empty
    elif df.empty:

        #df
        df=pd.DataFrame()

        #converted
        converted=False

    return df, converted


#SEARCH
folders=["zhao/_contributors_screen", "zhao/_search"]
items=["A_screen", "A_search"]
colname="a__company_involved"
#'''
def _search(folders, items, colname):

    #https://developers.refinitiv.com/en/article-catalog/article/building-search-into-your-application-workflow
    
    #https://github.com/Refinitiv-API-Samples/Article.DataLibrary.Python.Search/blob/main/Search%20-%20Query.ipynb
    #https://github.com/Refinitiv-API-Samples/Article.DataLibrary.Python.Search/blob/main/Search%20-%20Filter.ipynb

    #use SRCH to choose "filter" parameters

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #read csv
    filepath=f"{resources}/{resource}.csv"
    df_0=pd.read_csv(
        filepath, 
        dtype="string",
        #nrows=2,
        )
    
    #unique
    series=df_0[colname].unique()
    
    #drop na
    series=series.dropna()

    #sorted
    list_values=sorted(series)

    #trial
    #list_values=list_values[:10]

    #n obs
    n_obs=len(list_values)
    tot=n_obs-1
    frames=[None]*n_obs
    
    #select list
    select_list=[
        #name
        "DTSubjectName",
        "CommonName",

        #identifier
        "CUSIP",
        "IssueISIN",
        "RIC",
        "PrimaryRIC",
        "IssuerOAPermID",
        "PermID",
        "OAPermID",
        "Orgid",
        "TickerSymbol",

        #asset
        "AssetState", #active ('DC' if true)
        "BusinessEntity", #organization type
        "RCSOrganisationSubTypeLeaf",
        "OrganisationStatus", #listed
        "RCSAssetCategoryLeaf", #asset type (e.g., 'Ordinary Shares')

        #exchange
        "ExchangeName",
        "ExchangeCode",

        #ultimate parent
        "UltimateParentOrganisationName",
        "UltimateParentOrganisationOrgid",
        "UltimateParentCompanyOAPermID",

        #country
        "RCSIssuerCountryLeaf", #country of issuer
        "RCSExchangeCountryLeaf", #country of exchange
        "RCSFilingCountryLeaf", #country of incorporation
        ]

    #select   
    select=",".join(select_list)
    
    #top
    top=1

    #for names
    for i, query in enumerate(list_values):

        #clean query
        stardardized_query=_standardize_query(query)

        #try
        try:

            #EQUITY_QUOTES
            view=search.Views.EQUITY_QUOTES

            #filter - Type of Equity: Ordinary Shares
            filter="IsPrimaryIssueRIC eq true and \
                    \
                    RCSAssetCategoryGenealogy eq 'A:1L' and \
                    RCSIssuerCountryGenealogy eq 'M:DQ\\G:AM\\G:6J' and \
                    RCSExchangeCountryLeaf eq 'United States' \
                    " 
            df, converted = _search_loop(view, stardardized_query, filter, select, top, i, tot)

            #empty
            if df.empty:

                #ORGANISATIONS
                view=search.Views.ORGANISATIONS

                #filter - Organisation Type: Public Company
                filter="SearchAllCategoryv2 eq 'Companies/Issuers' and \
                        \
                        RCSFilingCountry xeq 'G:6J' \
                        " 
                df, converted = _search_loop(view, stardardized_query, filter, select, top, i, tot)

                #empty
                if df.empty:

                    #df
                    df=pd.DataFrame()

                    #converted
                    converted=False

                    #print
                    print(f"{i}/{tot} - {stardardized_query} - empty")
        
        #except
        except Exception as e:

            #df
            df=pd.DataFrame()

            #converted
            converted=False

            #print
            print(f"{i}/{tot} - {query} - exception")
            print(e)

        #create df0
        d={
            "query": [query],
            "stardardized_query": [stardardized_query],
            "converted": [converted],
            }
        df_1=pd.DataFrame(data=d)

        #create df_i
        df_i=pd.concat([df_1, df], axis="columns")

        #update frame
        frames[i]=df_i

    #concat
    df=pd.concat(frames)

    #reorder colnames
    df_1_cols=[
        "query",
        "stardardized_query",
        "converted",
        ]
    actual_selectlist=[col for col in select_list if col in df.columns]
    ordered_cols = df_1_cols + actual_selectlist 
    df=df[ordered_cols]

    #add initial df
    '''
    df=pd.merge(
        left=df_0,
        right=df,
        how="left",
        left_on=colname,
        right_on="query",
        suffixes=("_left", "_right"),
        indicator=True,
        validate="m:1",
        )
    #'''
    
    #save
    filepath=f"{results}/{result}_{colname}.csv"
    df.to_csv(filepath, index=False)




#copy to main.py
#'''
rdp.close_session()
rd.close_session()
print("_rdp - done")
#'''