

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
from _pd_utils import _clean_stem, _pd_DataFrame, _df_to_csvcols, _dfcol_to_listcol
from _standardize_names import _standardize_query


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


#retrieve n obs
def _n_obs(IDs, df):

    n_obs=0

    for j, ID in enumerate(IDs):
        col_name=ID[0]

        symbols_all=_dfcol_to_listcol(df, col_name)

        n_obs+=len(symbols_all)

    return n_obs


#divide in chunks
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


#from IDs to new symbols df
def _convert_IDs(df, IDs):
    #new symbols to find
    to_symbol_types=[e for e in SymbolTypes]
    enum_values=[e.value for e in SymbolTypes]

    #n obs
    n_obs=_n_obs(IDs, df)
    frames=[None]*n_obs

    i=0
    for j, ID in enumerate(IDs):

        #identifiers
        col_name=ID[0]
        from_symbol_type=ID[1]

        #retrieve symbols list
        symbols_all=_dfcol_to_listcol(df, col_name)

        #divide in chucks
        n_chunks=500
        chunks=chunker(symbols_all, n_chunks)

        for k, symbols in enumerate(chunks):
            try:
                df_i=rdp.convert_symbols(
                    symbols=symbols, 
                    from_symbol_type=from_symbol_type, 
                    to_symbol_types=to_symbol_types,
                    )
                    
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
            i+=1

    #frames
    df=pd.concat(frames)

    #rename index
    df=df.rename_axis("idx")

    return df


#CONVERT SYMBOLS
'''folders=["_convert_symbols0", "_convert_symbols1"]
items=["file_stem", "symbols"]
IDs=[
    ["isin", SymbolTypes.ISIN], 
    ["cusip", SymbolTypes.CUSIP],
    ]
#_convert_symbols(folders, items, IDs)
'''
#right click on import "SymbolTypes", "Go to Definition"
def _convert_symbols(folders, items, IDs):
    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]

    #read csv
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(filepath, dtype="string")

    #from IDs to new symbols df
    df=_convert_IDs(df, IDs)

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath)

    #for each column, create a csv
    _df_to_csvcols(df, results, result)



#CREATE PORTFOLIOS FOR REFINITIV WORKSPACE'S APP "PAL"
#folders=["_convert_symbols1", "_pal"]
#items=["symbols", "symbols"]
#_pal(folders, items)
def _pal(folders, items):
    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]

    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(filepath, dtype="string")

    col_name="RIC"
    symbols_all=_dfcol_to_listcol(df, col_name)

    n_chunks=200
    chunks=chunker(symbols_all, n_chunks)

    for i, symbols in enumerate(chunks): 
        values=[
            symbols,
            ]
        columns=[
            "Symbol",
            ]
        
        df=_pd_DataFrame(values, columns)

        filepath=f"{results}/{result}_pal_{i}.csv"
        df.to_csv(filepath, index=False)


#load items
def _load_items(resources, item):

    #index
    idx=item.rindex("_")

    #col names
    col_name=item[idx+1:]
    col_names=["fields", "parameters", "countries"]

    #if
    if col_name in col_names:
        pass

    #elif
    elif not (col_name in col_names):
        col_name="RIC"

    #read
    filepath=f"{resources}/{item}.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        )
    
    #df list
    df_list=df[col_name].to_list()

    #return
    return df_list


#get data loop
def _get_data_loop(instruments, fields, parameters, results):
    n_obs=len(instruments)*len(parameters)
    tot=n_obs-1

    frames=[None]*n_obs
    file_stems=[None]*n_obs
    converteds=[None]*n_obs

    i=0
    for j, instr in enumerate(instruments):
        print(j, instr)
        for k, param in enumerate(parameters):
            
            file_stem=f"{instr}_{param}"
            file_stem=_clean_stem(file_stem)
            output=Path(f"{results}/{file_stem}.csv")

            if output.is_file():
                df=pd.read_csv(output, dtype="string")
                df=df.set_index("file_stem")
                converted=True

                print(f"{i}/{tot} - {file_stem} - already done")
                

            elif not output.is_file():

                if pd.isna(instr):
                    df=pd.DataFrame()
                    converted=False

                    print(f"{i}/{tot} - {file_stem} - already missing in original db")  

                elif not pd.isna(instr):                    
                    param=json.loads(param)
                    
                    try:
                        df, err=ek.get_data(
                            instruments=instr, 
                            fields=fields, 
                            parameters=param, 
                            field_name=True,
                            raw_output=False,
                            debug=False,
                            )
                        
                        if err is not None:
                            err=err[0]
                            df=pd.DataFrame()
                            converted=False

                            print(f"{i}/{tot} - {file_stem} - error")
                            print(err)
                        
                        elif err is None:

                            if df is None:
                                df=pd.DataFrame()
                                converted=False
                                print(f"{i}/{tot} - {file_stem} - df none")
                            
                            elif df is not None:   
                                
                                if df.empty==True:
                                    df=pd.DataFrame()
                                    converted=False
                                    print(f"{i}/{tot} - {file_stem} - df empty")
                                
                                elif df.empty==False:
                                    df.insert(0, "file_stem", file_stem)
                                    df=df.set_index("file_stem")
                                    df.to_csv(output)
                                    converted=True

                                    print(f"{i}/{tot} - {file_stem} - done")
                                    time.sleep(1/5)

                    except Exception as e:
                        df=pd.DataFrame()
                        converted=False
                        
                        print(f"{i}/{tot} - {file_stem} - exception")
                        print(e)

            frames[i]=df
            file_stems[i]=file_stem
            converteds[i]=converted

            i+=1
            
    df=pd.concat(frames)
    df=df.reindex(index=file_stems)
    df.insert(1, "converted", converteds)  

    filepath=f"{results}/{results}.csv"
    df.to_csv(filepath)


def _print_msg(msg, j, tot_j, k, tot_k, jay, kay):
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

            file_stem=f"{jay}_{kay}"
            file_stem=_clean_stem(file_stem)
            output=Path(f"{results}/{file_stem}.csv")

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


#GET DATA
#folders=["_get_data0", "_get_data1"]
#items=["file_stem_fields", "file_stem_parameters", "file_stem_idx", "file_stem_countries"]
#_get_data(folders, items)
def _get_data(folders, items):
    resources=folders[0]
    results=folders[1]

    #fields
    item=items[0]
    fields=_load_items(resources, item)

    #parameters
    item=items[1]
    parameters=_load_items(resources, item)

    #idx
    item=items[2]
    instruments=_load_items(resources, item)
    
    #countries
    item=items[3]
    countries=_load_items(resources, item)  

    #screener - choose!
    #s_screener_loop(countries, fields, results)

    #get data loop - choose!
    _get_data_loop(instruments, fields, parameters, results)


def _extract_year(e):
    e=re.search(r"[0-9]{4}", e)
    e=e.group()
    e=str(e)
    return e


#RDP DATA FROM EXCEL
'''folders=["_get_data0", "_rdp_data1"]
items=["file_stem_idx", "file_stem_fields", "file_stem_parameters"]
col_symbol=["RIC", "RIC"]
col_symbol=["IssueISIN", "ISIN"]
#'''
def _rdp_data1(folders, items, col_symbol):

    #folders
    resources=folders[0]
    results=folders[1]

    col_name=col_symbol[0]
    symbol=col_symbol[1]

    #instruments_df
    item=items[0]
    filepath=f"{resources}/{item}.csv"
    instruments_df=pd.read_csv(
        filepath,
        dtype="string",
        )
    symbols_all=_dfcol_to_listcol(instruments_df, col_name)

    #fields_df
    item=items[1]
    filepath=f"{resources}/{item}.csv"
    fields_df=pd.read_csv(
        filepath,
        dtype="string",
        )

    #parameters
    item=items[2]
    parameters=_load_items(resources, item)
    parameters=[_extract_year(e) for e in parameters]

    #optional
    optional="CH=Fd RH=IN"

    #code
    code=f'CODE={symbol}'

    #chunks
    n_chunks=7000
    chunks=chunker(symbols_all, n_chunks)

    #init i
    i=0

    #for
    for j, param in enumerate(parameters):

        #for
        for k, chunk in enumerate(chunks):

            #wb
            wb=openpyxl.Workbook()

            #instruments chunk
            values=[
                chunk,
                ]
            columns=[
                col_name, 
                ]
            instruments_chunk=_pd_DataFrame(values, columns) 

            '''d={
                col_name: chunk,
                }
            instruments_chunk=pd.DataFrame(data=d) 
            #'''

            #sheets
            sheets={
                "instruments": instruments_chunk, 
                "fields": fields_df,
                }

            #for
            for key, value in sheets.items():
                title=key
                df=value

                #ws
                ws=wb.create_sheet(title=title)
                rows=dataframe_to_rows(df, index=True, header=True)

                #for
                for k, row in enumerate(rows):
                    ws.append(row)

            #rdp data
            rdp_data=f'=@RDP.Data(instruments!B3:B100000,fields!B3:B100,"Period={param} {optional} {code}")'

            #ws
            ws_data=wb.active
            ws_data.title=param
            ws_data["A1"]=rdp_data
            
            #save
            filepath=f"{results}/{param}_{i}.xlsx"
            wb.save(filepath)

            #print
            print(f"{param}_{i} - done")

            #update i
            i+=1


#CONCATATENATE EXCEL SHEETS FOR RDP DATA
#folders=["_rdp_data1", "_rdp_data2"]
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
        df=pd.read_excel(filepath)

        #rename instruments
        first_col=df.columns[0]
        df=df.rename(columns={first_col: "RIC"})

        file_stem=file.stem
        df.insert(0, "period", file_stem)

        frames[i]=df

        print(f"{i}/{tot} - {file} - done")

    df=pd.concat(frames)

    filepath=f"{results}/{results}.csv"
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
'''folders=["zhao/_contributors_screen", "zhao/_search"]
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
                view=search.Views.SEARCH_ALL
                #view=search.Views.ORGANISATIONS

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

    #reorder colnames
    ordered_cols=[col for col in select_list if col in df.columns]
    df=df[ordered_cols]
    
    #save
    filepath=f"{results}/{result}_{colname}.csv"
    df.to_csv(filepath, index=False)









#copy to main.py
rdp.close_session()
rd.close_session()
print("_rdp - done")
#'''