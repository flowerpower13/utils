

#imports
import pikepdf
from pathlib import Path
from pdfminer.high_level import extract_pages
from pdfminer.layout import LAParams, LTTextContainer


#functions
from _string_utils import _clean_text, _replace_txt
from _pd_utils import _pd_DataFrame, _folder_to_filestems


#variables
from _dict_bags import tuples_replace_beforeclean, tuples_replace_afterclean
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

    #write
    with open(
        output, 
        mode='w', 
        encoding="utf-8-sig", 
        errors="ignore",
        ) as file_object:
        file_object.write(text)

    return text


#read and write file
def _readwrite_txt(file, noncleaned_file):

    #read
    with open(
        file, 
        mode='r', 
        encoding="utf-8-sig", 
        errors="ignore",
        ) as file_object:
        text=file_object.read()

    #write
    with open(
        noncleaned_file, 
        mode='w', 
        encoding="utf-8-sig", 
        errors="ignore",
        ) as file_object:
        file_object.write(text)

    return text


#clean and save txt
def _cleansave_txt(noncleaned_text, output):

    #remove words before clean
    noncleaned_text=_replace_txt(noncleaned_text, tuples_replace_beforeclean)

    #clean text
    text=_clean_text(noncleaned_text)

    #remove words after clean
    text=_replace_txt(text, tuples_replace_afterclean)

    #write
    with open(
        output, 
        mode='w', 
        encoding="utf-8-sig", 
        errors="ignore",
        ) as file_object:
        file_object.write(text)


#from decrypted pdf to txt
def _pdf_to_txt(file, file_stem, file_suffix, output):

    #noncleaned file path
    noncleaned_file=Path(f"_noncleaned_txt/{file_stem}.txt")

    #decrypt and save
    if file_suffix==".pdf":

        #decrypt pdf to pdf
        decrypted_file=_decrypt_pdf(file, file_stem)

        #pdf to noncleaned txt
        noncleaned_text=_pdfminer(decrypted_file, noncleaned_file)

    #simple copy to txt
    elif file_suffix==".txt":

        #txt to noncleaned txt
        noncleaned_text=_readwrite_txt(file, noncleaned_file)

    #noncleaned txt to cleaned txt
    _cleansave_txt(noncleaned_text, output)


#from pdfs to txts
#create empty folder "_decrypt_pdf"
#folders=["_advev", "_pdfs_to_txts"]
def _pdfs_to_txts(folders, items): 
    resources=folders[0]
    results=folders[1]

    result=items[0]

    #colname
    colname_filestems="file_stem"

    #file stems
    files, file_stems=_folder_to_filestems(resources)

    #trial
    files=files[:2]
    file_stems=file_stems[:2]

    #n obs
    n_files=len(files)
    tot=n_files-1

    #empty lists
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
    file_path=f"{results}/{result}.csv"
    df.to_csv(file_path, index=False)
