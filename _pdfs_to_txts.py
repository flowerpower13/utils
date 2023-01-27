import shutil
import pikepdf
import pandas as pd
from pathlib import Path
from pdfminer.high_level import extract_pages
from pdfminer.layout import LAParams, LTTextContainer


from _pd_DataFrame import _pd_DataFrame
from _concat import _folder_to_filestems


#variables
error="???"
marker="###"


#decrypt pdf
def _decrypt_pdf(file, file_stem):

    #NEW file_path
    decrypted_file=Path(f"_decrypt_pdf/{file_stem}.pdf")

    #decrypt and save
    decrypted_pdf=pikepdf.open(file)
    decrypted_pdf.save(decrypted_file)

    return decrypted_file


#extract text from pdf
def _pdfminer(decrypted_file, output):

    #parameters
    laparams=LAParams(line_overlap=0.5, char_margin=2.0, line_margin=0.5, word_margin=0.1, boxes_flow=0.5, detect_vertical=False, all_texts=True)
    pages=extract_pages(decrypted_file, password='', page_numbers=None, maxpages=0, caching=True, laparams=laparams)
    
    #create empty text list
    text_list=[]

    #iterate through pae
    for i, page_layout in enumerate(pages):
        text_list.append(f"\n{marker}page_layout - {i}{marker}\n")
        
        for j, element in enumerate(page_layout):  
            if isinstance(element, LTTextContainer):

                try:
                    text_list.append(f"\n{marker}element.get_text() - {j}{marker}\n")
                    text_list.append(element.get_text())   
                
                except Exception as e:
                    print(e)
                    text_list.append(error)

    #text
    text=''.join(text_list)       

    #save
    with open(output, mode='w', encoding="utf-8-sig", errors="ignore") as file_object:
        file_object.write(text)


#from decrypted pdf to txt
def _pdf_to_txt(file, file_stem, file_suffix, output):

    #decrypt and save
    if file_suffix==".pdf":

        #decrypt pdf to pdf
        decrypted_file=_decrypt_pdf(file, file_stem)

        #pdf to txt
        _pdfminer(decrypted_file, output)

    #simple copy
    elif file_suffix==".txt":
        shutil.copy(file, output) 


#from pdfs to txts
def _pdfs_to_txts(folders): 
    resources=folders[0]
    results=folders[1]

    #colname
    colname_filestems="file_stem"

    #file stems
    files, file_stems=_folder_to_filestems(resources)

    #trial
    #files=files[:1]

    #n obs
    n_files=len(files)
    tot=n_files-1

    #ordered cols
    file_stems_ordered=[None]*n_files
    files_ordered=[None]*n_files
    outputs=[None]*n_files
    converteds=[None]*n_files

    for i, file in enumerate(files):

        #stem and suffix
        file_stem=file.stem
        file_suffix=file.suffix

        #output
        output=Path(f"{results}/{file_stem}.txt")

        #file is present
        if output.is_file():
            #converted
            converted=True

            #print
            print(f"{i}/{tot} - {file} - already done")

        #file is NOT present
        if not output.is_file():

            try:

                #try to convert
                _pdf_to_txt(file, file_stem, file_suffix, output)

                #converted
                converted=True

                #print
                print(f"{i}/{tot} - {file} - done")

            except Exception as e:

                #converted
                converted=False

                #print
                print(f"{i}/{tot} - {file} - error")
                print(f"{e}")

        #ordered cols
        file_stems_ordered[i]=file_stem
        files_ordered[i]=file
        outputs[i]=output
        converteds[i]=converted

    #create df
    values=[
        file_stems_ordered, 
        files_ordered, 
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
    file_path=f"{results}/_report.csv"
    df.to_csv(file_path, index=False)
