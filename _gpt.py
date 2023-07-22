

#link
#https://platform.openai.com/
#https://openai.com/pricing


#imports
from pathlib import Path
import pandas as pd
import openai
import math
import json

#functions
from _pd_utils import _pd_DataFrame, _folder_to_filestems


#setup
openai.organization="org-hWziYx3HcnnKO4wX4rpZitYH"
openai.api_key="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"



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
max_tokens=1
temperature=0
top_p=1
presence_penalty=-2
frequency_penalty=-2

#content_system_relevant
content_system_relevant="""
In the next message I will give you a text on the valuation of some companies, written by a valuation expert.
I want to know whether the text I gave contains information on the specific valuation methodologies employed by the expert.
If it does not, answer with a '0'. If the text does contain such information, answer with a '1'."""

#content_system_methods
content_system_methods="""
qwe
"""

method0="dcf"
method1="earnings_multiples"
method2="available_for_distribution"
method3="quoted_price"
method4="recent_genuine_offer"


#from txt to partitions
def _txt_to_partitions(text, max_chars=3500*4):

	#len text
	len_text=len(text)

	#n partitions
	n_partitions=math.ceil(len_text/max_chars)

	#empty partitions
	partitions=[None]*n_partitions

	#for
	for i in range(0, len_text, max_chars):

		#partition
		partition = text[i:i+max_chars]

		#append
		partitions[i]=partition
	
	return partitions


#from partitions to methods
def _partitions_to_methods(partitions):

	#relevant Y/N
	for i, partition in enumerate(partitions):

		messages=_contents_to_messages(
			content_system_relevant,
			partition,
			)

		#response
		relevant, completion_tokens = _input_to_gptresponse(
			model,
			messages,
			temperature,
			top_p,
			max_tokens,
			presence_penalty,
			frequency_penalty,
			)

		#if
		if relevant==0:

			#skip
			print("skip")
		
		#elif
		elif relevant==1:

			messages=_contents_to_messages(
				content_system_methods,
				partition,
				)

			#response
			response, completion_tokens = _input_to_gptresponse(
				model,
				messages,
				temperature,
				top_p,
				max_tokens,
				presence_penalty,
				frequency_penalty,
				)

			#dict methods
			dict_methods=json.loads(response)

			#empty dict
			methods=dict()

			#methods
			methods[method0]=dict_methods[method0]
			methods[method1]=dict_methods[method1]
			methods[method2]=dict_methods[method2]
			methods[method3]=dict_methods[method3]
			methods[method4]=dict_methods[method4]



	
	return methods



#from txt to df
def _txt_to_df(i, tot, file, file_stem, output):

	#try
	try:

		#open
		with open(
			file,
			mode="r",
			encoding="utf-8",
			errors="raise",	
			) as file_object:
			text=file_object.read()

		#split text below token limit
		partitions=_txt_to_partitions(text)

		#retrieve methods
		methods=_partitions_to_methods(partitions)

		#create df
		d={
			"file": file,
			"file_stem": file_stem,
			"output": output,
			method0: methods[method0],
			method2: methods[method1],
			method2: methods[method2],
			method3: methods[method3],
			method4: methods[method4],
			}
		df=pd.DataFrame(data=d)

		#save
		df.to_csv(output, index=False)

		#converted
		converted=True

		#print
		print(f"{i}/{tot} - {file} - done")

	#exception
	except Exception as e:

		#converted
		converted=False

		#print
		print(f"{i}/{tot} - {file} - exception")
		print(e)

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

	#colname
	colname_filestems="file_stem"

	#output suffix
	output_filesuffix="csv"

	#file stems
	files, file_stems=_folder_to_filestems(resources)

	#trial
	#files=files[:2]
	#file_stems=file_stems[:2]

	#n obs
	n_files=len(files)
	tot=n_files-1

	#empty lists
	outputs=[None]*n_files
	converteds=[None]*n_files

	#for
	for i, file in enumerate(files):

		#stem and suffix
		file_stem=file.stem
		file_suffix=file.suffix

		#output
		output=Path(f"{results}/{file_stem}.{output_filesuffix}")

		#file is present
		if output.is_file():

			#converted
			converted=True

			#print
			print(f"{i}/{tot} - {file} - already done")

		#file is NOT present
		if not output.is_file():

			#operation
			converted=_txt_to_df(i, tot, file, file_stem, output)

		#ordered cols
		outputs[i]=output
		converteds[i]=converted

	#create df
	values=[
		file_stems,
		files,
		outputs,
		converteds,
		]
	columns=[
		"file_stem",
		"file",
		"output",
		"converted",
		]
	df=_pd_DataFrame(values, columns)

	#sort
	df=df.sort_values(by=colname_filestems)

	#save
	filepath=f"{results}/{result}.csv"
	df.to_csv(filepath, index=False)


_txts_to_dfs(folders, items)
















print("done")