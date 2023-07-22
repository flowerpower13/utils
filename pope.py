
from _ids_to_reports import _ids_to_reports
from _pdfs_to_txts import _pdfs_to_txts


#Download list of IDs (press Excel logo), save in "_ids"
#Username: "UnivBocc", Password: "UbC4er53"
#https://www.connect4.com.au/subscribers/expertreports/?format=ajax&rawasx=&companyid=&screen=editCo&spindex=&industry_type_1=gics&industry_1=all&industry_type_2=gics&industry_2=all&industry_type_3=gics&industry_3=all&from_date=01%2F01%2F1991&to_date=24%2F11%2F2022&advisor_combinator=and&advisor_type_1=&advisor_type_2=&advisor_type_3=&advisor_type_4=&advisor_type_5=&advisor_type_6=&advisor_type_7=&advisor_type_8=&advisor_type_9=&advisor_type_10=&records=&other_party=&consideration_type=&conclusion=&l_d_val=&u_d_val=&report_combinator=and


#from IDs to pdf reports
folders=["_ids", "_ids_to_reports" ]
items=["c4_expertreports", "_ids_to_reports"]
#_ids_to_reports(folders, items)


#from pdf to txt
#create empty folder "_decrypt_pdf"
folders=["_ids_to_reports", "_pdfs_to_txts"]
#_pdfs_to_txts(folders)


#use ChatGPT to parse files


print("done")