

#virtual env
'''
#create
python -m venv pope

#activate
.\pope\Scripts\Activate.ps1

#close
deactivate

#export requirements
pip freeze > "pope/requirements.txt"

#install requirements
pip install -r "pope/requirements.txt"

python pope_functions.py
'''


#link
#https://platform.openai.com/


#imports
import json
import math
import openai
import numpy as np
import pandas as pd
from pathlib import Path


#functions
from _pdfs_to_txts import _pdfs_to_txts
from _string_utils import _simpleclean_text
from _ids_to_reports import _ids_to_reports
from _pd_utils import _pd_DataFrame, _folder_to_filestems


#vars
from _string_utils import ERROR, MARKER, ENCODING, ERRORS


#setup
from _appkey import openai_orgid, openai_apikey 
openai.organization=openai_orgid
openai.api_key=openai_apikey


#params
model="gpt-3.5-turbo"
temperature=0
max_tokens=500
token_limit=4096
max_chars=3000*4


#from content to messages
def _contents_to_messages(
	content_system,
	content_user,
	#content_assistant,
	):

	#messages
	message_system={
		"role": "system",
		"content": content_system,
		}
	message_user={
		"role": "user",
		"content": content_user,
		}
	#message_assistant={
		#"role": "assistant",
		#"content": content_assistant,
		#}

	#messages
	messages=[
	message_system,
	message_user,
	#message_assistant,
	]

    #return
	return messages


#from input to gpt ouput
def _input_to_gptresponse(
	model,
	messages,
	temperature,
	):

	#completion
	completion = openai.ChatCompletion.create(
		model=model,
		messages=messages,
		temperature=temperature,
		#top_p=1,
		#n=1,
		#stream=False,
		#stop=None,
		max_tokens=max_tokens,
		#presence_penalty=0,
		#frequency_penalty=0,
		#logit_bias=None,
		)

	#id, object, and created
	id=completion.id
	object=completion.object
	created=completion.created

	#choices
	choices=completion.choices

	index=choices[0].index
	role=choices[0].message.role
	response=choices[0].message.content
	finish_reason=choices[0].finish_reason

	#usage
	usage=completion.usage

	prompt_tokens=usage.prompt_tokens
	completion_tokens=usage.completion_tokens
	total_tokens=usage.total_tokens

	#warning finish reason
	if finish_reason!="stop":
		print(f"warning - finish_reason: {finish_reason}")

	if total_tokens>token_limit:
		print(f"warning - total_tokens: {total_tokens}>{token_limit}")

    #return
	return response, finish_reason, total_tokens


#content_system_relevant
content_system_relevant="""
In the next prompt I will give you a text on the valuation of some companies, written by a valuation expert.
I want to know whether the chunk of text I gave you contains information on the specific valuation methodologies employed by the valuation expert.
If it does not, answer with a "0". If the text does contain such information, answer with a "1".
"""

#content_system_methods
content_system_methods="""
Please indicate the valuation methodologies employed by the expert for the companies mentioned in the text below. The possible valuation methodologies are:
1. The discounted cash flow method and the estimated realizable value of any surplus assets.
2. The application of earnings multiples (appropriate to the business or industry in which the entity operates) to the estimated future maintainable earnings or cash flows of the entity, added to the estimated realizable value of any surplus assets.
3. The amount that would be available for distribution to security holders on an orderly realization of assets.
4. The quoted price for listed securities, when there is a liquid and active market and allowing for the fact that the quoted price may not reflect their value if 100% of the securities be available for sale.
5. Any recent genuine offers received by the target for any business units or assets as a basis for valuation of those business units or assets.

Your answer should be a  python dictionary with the following six keys: five for each valuation methodology mentioned above, and one key for a valuation methodology not mentioned above. Respectively, call the keys: 
1. "dcf", 
2. "earnings_multiples", 
3. "available_for_distribution",
4. "quoted_price",
5. "recent_genuine_offer", and
6. "alternative_valuation_methodology".

For the first five keys, in the dictionary assign value "1" if the valuation methodology has been employed, otherwise "0". 
It may be that the valuation expert is simply mentioning the valuation methodology, but not employing it for the valuation.
I need you to put value "1" whenever the valuation expert is actually employing the valuation methodology, not simply listing it.

For the last key "alternative_valuation_methodology" write the valuation methodology you find, if this methodology does not belong to the list of five above (e.g., "dividend discount model", other examples not listed above).
limit from 2 to 5 words XXX

"""




#methods
method0="dcf"
method1="earnings_multiples"
method2="available_for_distribution"
method3="quoted_price"
method4="recent_genuine_offer"


#from txt to partitions
def _txt_to_partitions(text, max_chars):

	#len text
	len_text=len(text)

	#n partitions
	n_partitions=math.ceil(len_text/max_chars)

	#empty partitions
	partitions=[None]*n_partitions

	#for
	i=0
	for j in range(0, len_text, max_chars):

		#partition
		partition=text[j:(j+max_chars)]

		#update partitions
		partitions[i]=partition

		#update i
		i+=1
	
    #return
	return partitions


#from partitions to methods
def _partitions_to_methods(partitions, output_txt):

	#initialize text
	text_list=list()

	#initialize relevant
	relevant_all=0

	#initialize dict
	methods={
		method0: 0,
		method1: 0,
		method2: 0,
		method3: 0,
		method4: 0,
		}

	#try
	try:
	
		#relevant Y/N
		for i, partition in enumerate(partitions):

			#partition text
			partition_text=f"\n\n{marker} partition - {i} {marker}\n{partition}\n\n"
			#update text list
			text_list.append(partition_text)

			#messages
			messages=_contents_to_messages(
				content_system=content_system_relevant,
				content_user=partition,
				)

			#try
			try:

				#response
				relevant, finish_reason, total_tokens = _input_to_gptresponse(
					model=model,
					messages=messages,
					temperature=temperature,
					max_tokens=max_tokens,
					)
				
				#response_type
				response_type="relevant"
				
				#finish_reason_text
				finish_reason_text=f"finish_reason: {finish_reason}"

				#total_tokens_text
				total_tokens_text=f"total_tokens: {total_tokens}"

				#relevant_text
				relevant_text=f"{response_type}: {relevant}"
				
				#text
				text=f"{marker} {response_type} - {i} {marker}\n{finish_reason_text}\n{total_tokens_text}\n{relevant_text}\n\n"
				#update text list
				text_list.append(text)

				print(text)

				#if
				if relevant=="0":
					pass
				
				#elif
				elif relevant=="1":

					#messages
					messages=_contents_to_messages(
						content_system=content_system_methods,
						content_user=partition,
						)	

					#response
					response, finish_reason, total_tokens = _input_to_gptresponse(
						model=model,
						messages=messages,
						temperature=temperature,
						max_tokens=max_tokens,
						)
					
					#response_type
					response_type="response"
					
					#finish_reason_text
					finish_reason_text=f"finish_reason: {finish_reason}"

					#total_tokens_text
					total_tokens_text=f"total_tokens: {total_tokens}"

					#relevant_text
					response_text=f"{response_type}: {response}"
					
					#text
					text=f"{marker} {response_type} - {i} {marker}\n{finish_reason_text}\n{total_tokens_text}\n{response_text}\n\n"
					#update text list
					text_list.append(text)

					print(text)

					#dict methods
					dict_methods=json.loads(response)

					#update methods
					methods[method0]+=dict_methods[method0]
					methods[method1]+=dict_methods[method1]
					methods[method2]+=dict_methods[method2]
					methods[method3]+=dict_methods[method3]
					methods[method4]+=dict_methods[method4]
					relevant_all+=int(relevant)

			#except
			except Exception as e:

				#print
				print(e)

		#converted
		converted=True

	except Exception as e:

		#converted
		converted=False

		#print
		print(e)

	#update text
	text=" ".join(text_list)

	#write
	with open(
		output_txt,
		mode="w",
		encoding="utf-8",
		errors="raise",	
		) as file_object:
		file_object.write(text)

	return relevant_all, methods, converted


#from txt to df
def _txt_to_df(i, tot, file, file_stem, output_csv, output_txt):

	#open_read
	with open(
		file,
		mode="r",
		encoding="utf-8",
		errors="raise",	
		) as file_object:
		text=file_object.read()
	
	#clean text
	text=_simpleclean_text(text)

	#split text below token limit
	partitions=_txt_to_partitions(text, max_chars)

	#retrieve methods
	relevant, methods, converted = _partitions_to_methods(partitions, output_txt)

	#if
	if converted==True:

		#create df
		d={
			"file": [file],
			"file_stem": [file_stem],
			"output_csv": [output_csv],
			"output_txt": [output_txt],
			"relevant": [relevant],
			method0: [methods[method0]],
			method1: [methods[method1]],
			method2: [methods[method2]],
			method3: [methods[method3]],
			method4: [methods[method4]],
			}
		df=pd.DataFrame(data=d)

		#save
		df.to_csv(output_csv, index=False)

		#print
		print(f"{i}/{tot} - {file} - done")

	#elif
	elif converted==False:
		
		#print
		print(f"{i}/{tot} - {file} - exception")

	return converted


#from txts to dfs
folders=["pope/_pdfs_to_txts", "pope/_txts_to_dfs"]
items=["_txts_to_dfs"]
def _txts_to_dfs(folders, items):

	#folders
	resources=folders[0]
	results=folders[1]

	#items
	result=items[0]

	#file stems
	files, file_stems=_folder_to_filestems(resources)

	#trial
	files=files[:1]
	file_stems=file_stems[:1]

	#n obs
	n_files=len(files)
	tot=n_files-1

	#empty lists
	output_csvs=[None]*n_files
	output_txts=[None]*n_files
	converteds=[None]*n_files

	#for
	for i, file in enumerate(files):

		#stem and suffix
		file_stem=file.stem

		#output
		output_csv=Path(f"{results}/{file_stem}.csv")
		output_txt=Path(f"{results}/{file_stem}.txt")

		#file is present
		if output_csv.is_file():

			#converted
			converted=True

			#print
			print(f"{i}/{tot} - {file} - already done")

		#file is NOT present
		if not output_csv.is_file():

			#operation
			converted=_txt_to_df(i, tot, file, file_stem, output_csv, output_txt)

		#ordered cols
		output_csvs[i]=output_csv
		output_txts[i]=output_txt
		converteds[i]=converted

	#create df
	d={
		"file_stem": file_stems,
		"file": files,
		"output_csvs": output_csvs,
		"output_txts": output_txts,
		"converted": converteds,	
		}
	df=pd.DataFrame(data=d)

	#sort
	df=df.sort_values(by="file_stem")

	#save
	filepath=f"{results}/{result}.csv"
	df.to_csv(filepath, index=False)











#Download list of IDs (press Excel logo), save in "_ids"
#Username: "UnivBocc", Password: "UbC4er53"
#https://www.connect4.com.au/subscribers/expertreports/?format=ajax&rawasx=&companyid=&screen=editCo&spindex=&industry_type_1=gics&industry_1=all&industry_type_2=gics&industry_2=all&industry_type_3=gics&industry_3=all&from_date=01%2F01%2F1991&to_date=24%2F11%2F2022&advisor_combinator=and&advisor_type_1=&advisor_type_2=&advisor_type_3=&advisor_type_4=&advisor_type_5=&advisor_type_6=&advisor_type_7=&advisor_type_8=&advisor_type_9=&advisor_type_10=&records=&other_party=&consideration_type=&conclusion=&l_d_val=&u_d_val=&report_combinator=and


#_ids_to_reports
folders=["pope/_ids", "pope/_ids_to_reports" ]
items=["c4_expertreports", "_ids_to_reports"]
#_ids_to_reports(folders, items)


#_pdfs_to_txts
#create empty folder "_decrypt_pdf"
folders=["pope/_ids_to_reports", "pope/_pdfs_to_txts"]
#_pdfs_to_txts(folders)


#_txts_to_dfs
#_txts_to_dfs(folders, items)






#print
print("done")









'''

typical formats, m and a
bookmarked or not? check properties






in the case of earnings multiple, which multiple P/E, or P/EBITDA, enterprise value

ask to rewrite the wording from the text, to quote

read 10

range of values

does the valuation expert make considerations related to climate/environmental risk?
100 top bigrams sautner, gavilidids

how does the valuation expert takes into account the risk of the companies?
what is the required rate of return or the discount rate?





write the email for the estimate

report types





phrases that distinguish pages with a table of contents
particular section
table of contents in multiple pages
check whether under 20 pages is report
first line of of TOC








https://github.com/drelhaj/CFIE-FRSE
https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2803275
'''



