from pathlib import Path 
import json
import textract
import os
import glob
import prodigy
import spacy
from textacy.preprocessing.normalize import normalize_whitespace,normalize_quotation_marks
import ujson
import re
from itertools import groupby
nlp=spacy.load("en_core_web_sm")

@prodigy.recipe("custom-docx-loader",
    source=prodigy.recipe_args['source'],
    normalize_ws=('Normalize whitespace', 'flag', 'ws', bool),
    mask_data=('mask numbers as NUM and date as DATE', 'flag', 'md', bool),
    to_lowercase=('lowercase sentences','flag','lc',bool),
    remove_special_characters=('remove characters', 'flag', 'rs', bool))
def load_data(source,normalize_ws=False,mask_data=False,to_lowercase=False,remove_special_characters=False):
    for filename in os.listdir(source):
        textArray = getDocText(source+filename)
        if(textArray['code']==200):
            textData=[]
            prv_sentence=''
            updated_strings=[]
            for line in textArray['data']:
                prv_sentence,is_prev_prepended=join_sentences(prv_sentence,line)
                if(is_prev_prepended == True):
                    updated_strings.pop()
                updated_strings.append(prv_sentence)
            
            for item in updated_strings:
                #item=item.text.replace("@DELIM@","").strip()
                if str(item).strip()=="":
                    continue
                else:
                    if(len(item.split()) > 5):
                        item = bytes(item, 'utf-8').decode('utf-8', 'ignore')
                        item = additional_processing(item,remove_special_characters,mask_data,to_lowercase)
                        if normalize_ws:
                            item = normalize_whitespace(item)
                        item = normalize_quotation_marks(item)
                        task = {"text": item,"meta":{"filename":filename}}
                        print(json.dumps(task))
                    
def getDocText(filepath):
    text_corpus = textract.process(filepath).decode("utf-8") 
    text_corpus=text_corpus.replace("\u00a0"," ")
    textArray = text_corpus.split("\n")

    dataset=[]
    for text in textArray:
        if str(text).strip()=="":
            continue
        else:
            dataset.append(str(text))#.lstrip('0123456789.- ')

    dataset = list(dict.fromkeys(dataset))

    return {"code":200,"data":dataset,"message":""}
    
def join_sentences(prv_sentence,current_sentence):
    is_prev_prepended=False
    end_tokens=[".",",",":",";","”","and"]
    if(prv_sentence !=  "" and list(prv_sentence.strip())[-1] not in end_tokens):
        if(list(current_sentence)[0] == " "):
            updated_line=prv_sentence+current_sentence
        else:
            updated_line=prv_sentence+" "+current_sentence
        is_prev_prepended=True
    else:
        updated_line=current_sentence

    return updated_line,is_prev_prepended

def additional_processing(text,remove_special_characters=False,mask_data=False,to_lowercase=False):
    if remove_special_characters:
        text=re.sub('[^a-zA-Z0-9 \n\.,()]', ' ', text)
    if mask_data:
        doc=nlp(text)
        for ent in doc.ents:
            if(ent.label_ == "DATE" or ent.label_=="CARDINAL"):
                text = text.replace(ent.text,ent.label_)
        text=re.sub(r'\d+\.\d+', 'NUM', text)
        text=re.sub('\d', 'NUM', text)
        text=text.replace('CARDINAL', 'NUM')
        while "NUMNUM" in text:
            text=text.replace("NUMNUM","NUM")
        while "NUM NUM" in text:
            text=text.replace("NUM NUM","NUM")
        while "NUM , NUM" in text:
            text=text.replace("NUM , NUM","NUM")
        while "NUM.NUM" in text:
            text=text.replace("NUM.NUM","NUM")
        text=text.replace("NUM"," NUM ") 
        text=text.replace("(the","( the")
        text=text.replace("Date,","Date ,")
        text=text.replace("Closing,","Closing ,")
        text=text.replace("(i)",",")
        text=text.replace("(ii)",",")
        text=text.replace("(a)",",")
        text=text.replace("(b)",",")
        text=text.replace("provided,","provided ,")
        text=text.replace("however,","however ,")
        text=text.replace("Agreement,","Agreement ,")
        text=text.replace("(other","( other)")   
        text=text.replace("  "," ")
        
        text = " ".join([k for k,v in groupby(text.split())])
    if(to_lowercase == True):
            text = text.lower()
    return text
