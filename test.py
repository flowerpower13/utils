

import refinitiv.data as rd
from refinitiv.data.content import search
appkey="7203cad580454a948f17be1b595ef4884be257be"
rd.open_session(app_key=appkey)


view=search.Views.EQUITY_QUOTES
filter="IsPrimaryIssueRIC eq true and \
        \
        RCSAssetCategoryGenealogy eq 'A:1L' and \
        RCSIssuerCountryGenealogy eq 'M:DQ\\G:AM\\G:6J' and \
        RCSExchangeCountryLeaf eq 'United States' \
        " 

#select
select="DTSubjectName, AssetState, CUSIP, IssueISIN, RIC, IssuerOAPermID, PermID, TickerSymbol, \
        ExchangeName, ExchangeCode, \
        \
        RCSAssetCategoryLeaf, \
        RCSIssuerCountryLeaf, \
        RCSExchangeCountryLeaf, \
        "

top=1

query="microsoft"

df=rd.discovery.search(
    view=view,
    query=query,
    filter=filter,
    select=select,
    top=top,
    )

print(df)

rd.close_session()
print("_rdp - done")
#'''
