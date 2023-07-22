

#link
#https://platform.openai.com/
#https://openai.com/pricing


#imports
from pathlib import Path
import pandas as pd
import numpy as np
import openai
import math
import json


#functions
from _pd_utils import _pd_DataFrame, _folder_to_filestems
from _string_utils import _simpleclean_text


#vars
from _string_utils import error, marker, encoding, errors


#setup
from _appkey import openai_orgid, openai_apikey 
openai.organization=openai_orgid
openai.api_key=openai_apikey






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

	return messages


#from input to gpt ouput
def _input_to_gptresponse(
	model,
	messages,
	temperature,
	top_p,
	max_tokens,
	presence_penalty,
	frequency_penalty,
	):

	#completion
	completion = openai.ChatCompletion.create(
		model=model,
		messages=messages,
		temperature=temperature,
		top_p=top_p,
		#n=1,
		#stream=False,
		#stop=None,
		max_tokens=max_tokens,
		presence_penalty=presence_penalty,
		frequency_penalty=frequency_penalty,
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

	return response, completion_tokens


#vars
model="gpt-3.5-turbo"
temperature=0
top_p=1
presence_penalty=-2
frequency_penalty=-2

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

Your answer should be a  python dictionary with the following 5 keys, one for each valuation methodology. Respectively, call the keys: 
1. "dcf", 
2. "earnings_multiples", 
3. "available_for_distribution",
4. "quoted_price", and 
5. "recent_genuine_offer".

For each key above, in the dictionary assign value "1" if the valuation methodology has been employed, otherwise 0.
It may be that the valuation expert is simply mentioning the valuation methodology, but not employing it for the valuation.
I need you to put value "1" whenever the valuation expert is actually employing the valuation methodology, not simply listing it.
"""


#methods
method0="dcf"
method1="earnings_multiples"
method2="available_for_distribution"
method3="quoted_price"
method4="recent_genuine_offer"


#from txt to partitions
def _txt_to_partitions(text, max_chars=3000*4):

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
			partition_text=f"\n\n\n{marker} partition - {i} {marker}\n{partition}\n\n"
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
				max_tokens=1
				relevant, completion_tokens = _input_to_gptresponse(
					model,
					messages,
					temperature,
					top_p,
					max_tokens,
					presence_penalty,
					frequency_penalty,
					)
				
				#relevant text
				relevant_text=f"{marker} relevant - {i} {marker}\n{relevant}\n\n"
				#update text list
				text_list.append(relevant_text)

				#if
				if relevant=="0":

					#skip
					print("skip")
				
				#elif
				elif relevant=="1":

					#messages
					messages=_contents_to_messages(
						content_system=content_system_methods,
						content_user=partition,
						)	

					#response
					max_tokens=100
					response, completion_tokens = _input_to_gptresponse(
						model,
						messages,
						temperature,
						top_p,
						max_tokens,
						presence_penalty,
						frequency_penalty,
						)
					
					#response text
					response_text=f"{marker} response - {i} {marker}\n{response}\n\n"
					#update text list
					text_list.append(response_text)

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

	#read
	with open(
		file,
		mode="r",
		encoding="utf-8",
		errors="raise",	
		) as file_object:
		text=file_object.read()
	
	#clean text
	text=_simpleclean_text(text)

	#trial
	text=text[:100]

	text="""
	I employ the discounted cash flows model, but not the earnings multiples
	"""

	#split text below token limit
	partitions=_txt_to_partitions(text)

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
			method2: [methods[method1]],
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


_txts_to_dfs(folders, items)
















print("done")