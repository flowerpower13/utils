

#imports
import pikepdf
from pathlib import Path
from pdfminer.high_level import extract_pages
from pdfminer.layout import LAParams, LTTextContainer


#functions
from _string_utils import _clean_text, _replace_txt
from _pd_utils import _pd_DataFrame, _folder_to_filestems


#variables
from _hassan_vars import tuples_replace_beforeclean, tuples_replace_afterclean
from _string_utils import error, marker
encoding="utf-8"
errors="strict"

#decrypt pdf
def _decrypt_pdf(file, file_stem):

    #NEW file_path
    decrypted_file=Path(f"_decrypt_pdf/{file_stem}.pdf")

    #try
    try:

        #decrypt and save
        decrypted_pdf=pikepdf.open(file)
        decrypted_pdf.save(decrypted_file)
    
    except Exception as e:
        print(e)
        print(decrypted_file)

    return decrypted_file


#extract text from pdf
def _pdfminer(decrypted_file, output):

    #parameters
    laparams=LAParams(line_overlap=0.5, char_margin=2.0, line_margin=0.5, word_margin=0.1, boxes_flow=0.5, detect_vertical=False, all_texts=True)

    #create empty text list
    text_list=[]

    #extract pages
    pages=extract_pages(decrypted_file, password='', page_numbers=None, maxpages=0, caching=True, laparams=laparams)
    
    #for page
    for i, page_layout in enumerate(pages):

        #marker page
        marker_page=f"\n{marker}page_layout - {i}{marker}\n"
        text_list.append(marker_page)
        
        #for element
        for j, element in enumerate(page_layout): 

            #is instance
            if isinstance(element, LTTextContainer):

                #marker element
                marker_element=f"\n{marker}element.get_text() - {j}{marker}\n"
                text_list.append(marker_element)

                #try
                try:
                    #text element
                    text_element=element.get_text()
                    text_list.append(text_element)   
                
                except Exception as e:
                    print(e)
                    text_list.append(error)

    #text
    text=' '.join(text_list)       

    #write
    with open(
        output, 
        mode='w', 
        encoding=encoding, 
        errors=errors,
        ) as file_object:
        file_object.write(text)

    return text


#read and write file
def _readwrite_txt(file, raw_file):

    #read
    with open(
        file, 
        mode='r', 
        encoding=encoding, 
        errors=errors,
        ) as file_object:
        raw_text=file_object.read()

    #write
    with open(
        raw_file, 
        mode='w', 
        encoding=encoding, 
        errors=errors,
        ) as file_object:
        file_object.write(raw_text)

    return raw_text


#clean and save txt
def _cleansave_txt(raw_text, output):

    #remove words before clean
    raw_text=_replace_txt(raw_text, tuples_replace_beforeclean)

    #clean text
    text=_clean_text(raw_text)

    #remove words after clean
    text=_replace_txt(text, tuples_replace_afterclean)

    #write
    with open(
        output, 
        mode='w', 
        encoding=encoding, 
        errors=errors,
        ) as file_object:
        file_object.write(text)


#from decrypted pdf to txt
def _pdf_to_txt(file, file_stem, file_suffix, output):

    #raw file path
    raw_file=Path(f"_raw_txt/{file_stem}.txt")

    #try
    try:

        #decrypt and save
        if file_suffix==".pdf":

            #decrypt and convert
            decrypted_file=_decrypt_pdf(file, file_stem)
            raw_text=_pdfminer(decrypted_file, raw_file)

            #converted
            converted=True

        #simple copy to txt
        elif file_suffix==".txt":

            #txt to raw txt
            raw_text=_readwrite_txt(file, raw_file)

        #raw txt to cleaned txt
        _cleansave_txt(raw_text, output)
    
    except Exception as e:
        print(e)
        print(file)

        #converted
        converted=False

    return converted


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
    #files=files[:2]
    #file_stems=file_stems[:2]

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

            #try to convert
            converted=_pdf_to_txt(file, file_stem, file_suffix, output)

            #print
            print(f"{i}/{tot} - {file} - done")

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
    