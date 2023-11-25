

#virtual env
'''
#create
python -m venv ciani

#activate
.\ciani\Scripts\Activate.ps1

#close
deactivate

#export requirements
pip freeze > requirements.txt

#install requirements
pip install -r requirements.txt

python ciani_functions.py
'''


#import
#pip install pandas
#pip show pandas
import pandas as pd


#function
from _pd_utils import _folder_to_filestems


#run
def _run():

    #OpenPayments
    #https://www.cms.gov/priorities/key-initiatives/open-payments/data/dataset-downloads
    filepath="ciani/data/OP_DTL_GNRL_PGYR2022_P06302023.csv"
    usecols=[
        "Change_Type",
        "Covered_Recipient_Type",
        "Teaching_Hospital_CCN",
        "Teaching_Hospital_ID",
        "Teaching_Hospital_Name",
        "Covered_Recipient_Profile_ID",
        "Covered_Recipient_NPI",
        "Covered_Recipient_First_Name",
        "Covered_Recipient_Middle_Name",
        "Covered_Recipient_Last_Name",
        "Covered_Recipient_Name_Suffix",
        "Recipient_Primary_Business_Street_Address_Line1",
        "Recipient_Primary_Business_Street_Address_Line2",
        "Recipient_City",
        "Recipient_State",
        "Recipient_Zip_Code",
        "Recipient_Country",
        "Recipient_Province",
        "Recipient_Postal_Code",
        "Covered_Recipient_Primary_Type_1",
        "Covered_Recipient_Primary_Type_2",
        "Covered_Recipient_Primary_Type_3",
        "Covered_Recipient_Primary_Type_4",
        "Covered_Recipient_Primary_Type_5",
        "Covered_Recipient_Primary_Type_6",
        "Covered_Recipient_Specialty_1",
        "Covered_Recipient_Specialty_2",
        "Covered_Recipient_Specialty_3",
        "Covered_Recipient_Specialty_4",
        "Covered_Recipient_Specialty_5",
        "Covered_Recipient_Specialty_6",
        "Covered_Recipient_License_State_code1",
        "Covered_Recipient_License_State_code2",
        "Covered_Recipient_License_State_code3",
        "Covered_Recipient_License_State_code4",
        "Covered_Recipient_License_State_code5",
        "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name",
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID",
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State",
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country",
        "Total_Amount_of_Payment_USDollars",
        "Date_of_Payment",
        "Number_of_Payments_Included_in_Total_Amount",
        "Form_of_Payment_or_Transfer_of_Value",
        "Nature_of_Payment_or_Transfer_of_Value",
        "City_of_Travel",
        "State_of_Travel",
        "Country_of_Travel",
        "Physician_Ownership_Indicator",
        "Third_Party_Payment_Recipient_Indicator",
        "Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value",
        "Charity_Indicator",
        "Third_Party_Equals_Covered_Recipient_Indicator",
        "Contextual_Information",
        "Delay_in_Publication_Indicator",
        "Record_ID",
        "Dispute_Status_for_Publication",
        "Related_Product_Indicator",
        "Covered_or_Noncovered_Indicator_1",
        "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1",
        "Product_Category_or_Therapeutic_Area_1",
        "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1",
        "Associated_Drug_or_Biological_NDC_1",
        "Associated_Device_or_Medical_Supply_PDI_1",
        "Covered_or_Noncovered_Indicator_2",
        "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2",
        "Product_Category_or_Therapeutic_Area_2",
        "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2",
        "Associated_Drug_or_Biological_NDC_2",
        "Associated_Device_or_Medical_Supply_PDI_2",
        "Covered_or_Noncovered_Indicator_3",
        "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3",
        "Product_Category_or_Therapeutic_Area_3",
        "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3",
        "Associated_Drug_or_Biological_NDC_3",
        "Associated_Device_or_Medical_Supply_PDI_3",
        "Covered_or_Noncovered_Indicator_4",
        "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4",
        "Product_Category_or_Therapeutic_Area_4",
        "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4",
        "Associated_Drug_or_Biological_NDC_4",
        "Associated_Device_or_Medical_Supply_PDI_4",
        "Covered_or_Noncovered_Indicator_5",
        "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5",
        "Product_Category_or_Therapeutic_Area_5",
        "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5",
        "Associated_Drug_or_Biological_NDC_5",
        "Associated_Device_or_Medical_Supply_PDI_5",
        "Program_Year",
        "Payment_Publication_Date",
        ]

    #read_csv
    df=pd.read_csv(
        filepath,
        usecols=usecols,
        dtype="string",
        nrows=10,
        )
    
    #to_csv
    filepath="ciani/data/openpayments.csv"
    df.to_csv(
        filepath,
        index=False,
        )

    #Medicare Part D Prescribers - by Provider and Drug
    #https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug
    year=2021
    filepath=f"ciani/data/Medicare_Part_D_Prescribers_by_Provider_and_Drug_{year}.csv"
    usecols=[
        "Prscrbr_NPI",
        "Prscrbr_Last_Org_Name",
        "Prscrbr_First_Name",
        "Prscrbr_City",
        "Prscrbr_State_Abrvtn",
        "Prscrbr_State_FIPS",
        "Prscrbr_Type",
        "Prscrbr_Type_Src",
        "Brnd_Name",
        "Gnrc_Name",
        "Tot_Clms",
        "Tot_30day_Fills",
        "Tot_Day_Suply",
        "Tot_Drug_Cst",
        "Tot_Benes",
        "GE65_Sprsn_Flag",
        "GE65_Tot_Clms",
        "GE65_Tot_30day_Fills",
        "GE65_Tot_Drug_Cst",
        "GE65_Tot_Day_Suply",
        "GE65_Bene_Sprsn_Flag",
        "GE65_Tot_Benes",
        ]

    #read_csv
    df=pd.read_csv(
        filepath,
        usecols=usecols,
        dtype="string",
        nrows=10,
        )
    
    #to_csv
    filepath="ciani/data/partd.csv"
    df.to_csv(
        filepath,
        index=False,
        )

#run
_run()


#print
print("done")



#feedback
'''
free trial? source/methodology, which variables?
1. list of all pharmaceutical firms operating in US and EU, with ISIN code, GICS,
2. phase II trials, in each therapeutic area, in year t,
3. n of firm-level clinical trials in US, and in EU, in each year,
4. n of patents issue by each pharma firm.

Life cycle
installments - staggered payments - staged payments - split payments - annuities - payments over time - performance-linked payments


budish, clinical trials in AER, clinical trials other websites, eu trials, 
trial registry cochrane, marketing pfizer, drug patent about to expire (or from FDA approval to patent expiration)
eu federation of pharma industry, flatiron


https://www.factset.com/marketplace/catalog/product/factset-streetaccount-drug-events-and-pipeline
https://healthitanalytics.com/news/do-data-transparency-open-payments-reduce-clinical-research
https://www.prnewswire.com/news-releases/new-data-suggest-downward-trend-in-clinical-research-spending-300129781.html
https://www.policymed.com/2014/04/physician-payment-sunshine-act-effect-on-smaller-companies.html
https://doi.org/10.1001/jama.2019.8171 (lack in trust)

pharma firms and AGs, repeal obamacare

especially on specialty drugs
organization by doctors
insurance companies want 

%0. LHS: PAYMENT_ijskt: RHS: PHYSICIANCONTROLS_jst. FE_k
firm i, physician j, state s, market k, time t
obamacare

%1. LHS: PAYMENT_ijskt, RHS: MIGRATORYSHOCK_st. FE_k
stag DID

%2. LHS: PRESCRIPTION_ijskt, RHS: PAYMENT_ijskt X DISEASESHOCK_st, FE_k
PRESCRIPTION_ijskt=#$drugs produced by i prescribed by j (in state s in market k)/-i

%3. LHS: MP_it, RHS: PAYMENT_ijskt X UNCERTAINTY_t, FE_k
MP_it=Lerner Index or ROA

%year of first patent. find USPTO
%quality of drug: clinical trials goodness of study p hacking, are they randomized study, controlled or not, if comparator placebo or active, outcome surrogate or final/clinical (%fda pivotal trial, ) as RHS. LHS payment (story: firms pay more for weaker drugs), assessment of clinical trials supporting us fda approval of novel therapeuitic agents


'''

#open payments faqs

#Medicare Physician & Other Practitioners - by Provider and Service (how much powerful the doctor?)
#https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service


#NPPES (to get physicians' name not in OpenPayments)
#https://download.cms.gov/nppes/NPI_Files.html
#https://download.cms.gov/nppes/NPPES_Data_Dissemination_October_2023.zip


#linktransformer (to merge physicians' names)
#https://linktransformer.github.io/


#therapeutic areas for physicians (as in OpenPayments)
#https://www.fda.gov/drugs/development-resources/spectrum-diseasesconditions


#NDC (drug codes as in OpenPayments)
#https://www.fda.gov/industry/structured-product-labeling-resources/nsde
#https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-directory


#WHO's ATC (from drug names in Medicare part D to therapeutic area)
#https://www.whocc.no/atc_ddd_index/
#https://bioportal.bioontology.org/ontologies/ATC
#https://github.com/fabkury/atcd/


#clinical trials
#https://clinicaltrials.gov/


#MeSH (therapeutic areas for clinical trials)
#https://www.nlm.nih.gov/mesh/meshhome.html


#FDA
#https://datadashboard.fda.gov/ora/index.htm
#https://open.fda.gov/apis/drug/


#Medicare
#https://data.cms.gov/search
