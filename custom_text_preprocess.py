import prodigy
import spacy
from textacy.preprocessing.normalize import normalize_whitespace,normalize_quotation_marks
import ujson
import re
from itertools import groupby
nlp=spacy.load("en_core_web_sm")
@prodigy.recipe('preprocess',
    source=prodigy.recipe_args['source'],
    normalize_ws=('Normalize whitespace', 'flag', 'ws', bool),
    fix_unicode=('Fix broken unicode', 'flag', 'u', bool),
    mask_data=('mask numbers as NUM and date as DATE', 'flag', 'md', bool),
    to_lowercase=('lowercase sentences','flag','lc',bool),
    remove_special_characters=('remove characters', 'flag', 'rs', bool))
def preprocess(source, normalize_ws=False, fix_unicode=False, mask_data =False,to_lowercase=False,remove_special_characters=False):
    stream = prodigy.get_stream(source)
    for eg in stream:
        text = eg['text']
        text = additional_processing(text,remove_special_characters,mask_data,to_lowercase)
        if normalize_ws:
            text = normalize_whitespace(text)
        text = normalize_quotation_marks(text)
        
        eg['text'] = text
        # write example to stdout
        print(ujson.dumps(eg, escape_forward_slashes=False, ensure_ascii=False))


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
        while "NUM.NUM" in text:
            text=text.replace("NUM.NUM","NUM")
        text=text.replace("NUM"," NUM ") 
        text=text.replace("(","( ")
        text=text.replace(")",") ")
        text=text.replace(","," ,")
        text=text.replace("Closing,","Closing ,")
        text=text.replace("(i)",",")
        text=text.replace("(ii)",",")
        text=text.replace("(a)",",")
        text=text.replace("(b)",",")
        while "  " in text:
            text=text.replace("  "," ")
        
        text = " ".join([k for k,v in groupby(text.split())])
    if(to_lowercase == True):
            text = text.lower()
    return text
