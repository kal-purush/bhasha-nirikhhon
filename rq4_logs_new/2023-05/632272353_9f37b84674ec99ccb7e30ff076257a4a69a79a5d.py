from googletrans import Translator
from langdetect import detect
import re

letters = {
    "A": "ఏ", "B": "బీ", "C": "సీ", "D": "డీ",
    "E": "ఈ", "F": "ఎఫ్", "G": "జీ", "H": "హెచ్",
    "I": "ఐ", "J": "జే", "K": "కే", "L": "ఎల్",
    "M": "ఎం", "N": "ఎన్", "O": "ఓ", "P": "పీ",
    "Q": "క్యూ", "R": "ఆర్", "S": "ఎస్", "T": "టీ",
                 "U": "యూ", "V": "వీ", "W": "డబల్యూ", "X": "ఎక్స్",
                 "Y": "వై", "Z": "జెడ్", '0': '0', '1': '1', '2': '2',
                 '3': '3', '4': '4', '5': '5', '6': '6', '7': '7', '8': '8',
                 '9': '9', '_': '_'
}

data_type = {
    "char6 3": "చార్గా","int6 3": "పూర్ణంగా","short6 3": "చిన్నదిగా",
    "long6 3": "పెద్దదిగా","float6 3": "ఫ్లోట్గా","double6 3": "డబల్గా",
    "void6 3": "ఖాళీగా","unsigned6 3": "సానుకూల సంఖ్యగా","signed6 3": "ప్రతికూల సంఖ్యగా",
    "6 3":"గా", 
    
    "char6":"చార్కి","int6":"పూర్ణంకి","short6":"చిన్నదికి",
    "long6":"పెద్దదికి","float6":"ఫ్లోట్కి","double6":"డబల్కి",
    "void6":"ఖాళీకి","unsigned6":"సానుకూల సంఖ్యకి","signed6":"ప్రతికూల సంఖ్యకి",
    "6 ":"కి ", 
    
    "char":"చార్","int":"పూర్ణం","short":"చిన్నది",
    "long":"పెద్దది","float":"ఫ్లోట్","double":"డబల్",
    "void":"ఖాళీ","unsigned":"సానుకూల సంఖ్య","signed":"ప్రతికూల సంఖ్య",
    
    "pointer to char":"చార్కి పాయింటర్గా","pointer to int":"పూర్ణంకి పాయింటర్గా",
    "pointer to short":"చిన్నదికి పాయింటర్గా","pointer to long":"పెద్దదికి పాయింటర్గా",
    "pointer to float":"ఫ్లోట్కి పాయింటర్గా","pointer to double":"డబల్కి పాయింటర్గా",
    "pointer to void":"ఖాళీకి పాయింటర్గా","pointer to unsigned":"సానుకూల సంఖ్యకి పాయింటర్గా",
    "pointer to signed":"ప్రతికూల సంఖ్యకి పాయింటర్గా","args": "ఆర్గ్స్",

    "auto2":"ఆటో", "extern2":"ఎక్స్టర్న్‌ ", 
    "static2":"స్టాటిక్‌", "register2":"రిజిస్టర్‌", 
    
    "volatile4":"వోలటైల్‌", "volatile5":"వోలటైల్", 
    "const": "స్థిరమైన","const4": "స్థిరమైన",
    "const5": "స్థిరమైన",

    " as1 ":" ను ",
    "declare":"ప్రకటించండి",
    "pointer to 3":"పాయింటర్గా",
    "pointer to":"పాయింటర్కి",
    "array of":"శ్రేణి యొక్క", 
    "array ":" శ్రేణి ",
    " of 3 ":" గా ",
    " of ":" కి ",
    "returning":"తిరిగి",
    "function":"ఫంక్షన్",
    "syntax error":"సింటాక్స్ లోపం",
    "bad character":"చెడు అక్షరం",
    

    "":"", 
    " var":" వార్‌",
    "enum":"ఇనమ్‌", 
    "union":"యూనియన్",
    "struct": "నిర్మాణం",
    '(int)': "(పూర్ణం)",
    "counst": "స్థిరంగా",
    "chant": "స్థిరంగా",
    "ch1": "ch1",
    "lhjb": "lhjb"
}

def transt(text):
    if text == "syntax error":
        return data_type[text]
    sen_list = text.split()
    var = sen_list[0]
    # u_d_type = sen_list[-3][:-1]
    if sen_list[-1]=="var":
        var = sen_list[-2]
    telugu_text = replace_words_with_values(text,data_type)
    telugu_text  = telugu_text.replace(var,variable(var))
    # telugu_text.replace('chh',variable(u_d_type[:-1]))
    telugu_text = transt1(telugu_text)
    # telugu_text = telugu_text.replace(u_d_type,variable(u_d_type))
    try:
        eng_w=eng_find(telugu_text)
        telugu_text = telugu_text.replace(eng_w,variable(eng_w))
    except:
        pass
    return telugu_text

def variable(tv):
    # print(tv)
    if len(tv)>2:
        translator = Translator(service_urls=['translate.google.com'])
        tv = translator.translate(tv, dest='te').text
    if detect(tv) != 'te':
        splt = list(tv)
        emptstr = ''
        for i in splt:
            try:
                emptstr += letters[i.upper()]
            except:
                pass
        return emptstr
    else:
        return tv

def replace_words_with_values(string, dictionary):
    pattern = re.compile(r'\b({})\b'.format('|'.join(map(re.escape, dictionary.keys()))))
    replaced_string = pattern.sub(lambda x: dictionary[x.group()], string)
    return replaced_string 

# def replace_words_with_values1(string, dictionary):
#     replaced_words = []
#     for word in string.split():
#         replaced_word = dictionary.get(word, word)
#         replaced_words.append(replaced_word)
#     result = ' '.join(replaced_words)
#     return result

def transt1(text):
    text = re.sub(r'6 3', 'గా', text)
    text = re.sub(r'6 ', 'కి ', text)
    telugu_text = replace_words_with_values(text, data_type)
    return telugu_text  

def eng_find(text):
    splt = text.split()
    eng_txt = ''
    try:
        for i in splt: 
            if detect(i)!='te':
                eng_txt = i[:-2]
    except:
        pass
    if eng_txt=='':
        for i in text:
            if i == ' 'or i.isnumeric():
                continue
            if detect(i)!='te':
                eng_txt+=i
    return eng_txt
# x = input("enter the string:")
# # print("translated string:"+transt(x))
# a,b=transt(x)

# with open("output.txt","a") as f:
#     f.write(a+"\n"+b+"\n")