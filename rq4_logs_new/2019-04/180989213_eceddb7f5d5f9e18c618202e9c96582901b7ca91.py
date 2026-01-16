#!/usr/bin/python3

nlu_config="""
language: "en"

pipeline:
- name: tokenizer_whitespace
- name: intent_entity_featurizer_regex
- name: ner_crf
- name: ner_synonyms
"""
#=======================Rasa libraries=================================
from rasa_nlu.model import Interpreter
from rasa_nlu.model import InvalidProjectError as rasa_model_error
#=======================OS Modules=====================================
from os import system
from os import mkdir
from os.path import isdir
from shutil import rmtree as rmdir
from sys import version
import pickle
from pprint import pprint
from fuzzywuzzy import process

class UserInterruptError(Exception):
    pass

def extract_entities(line):

    detected_intents=[]
    index=0
    count=0
    while line[index:].find('(')!=-1 :
        intent=line[index:][line[index:].find('(')+1:line[index:].find(')')]
        if intent not in detected_intents :
            detected_intents.append(intent)
            print(intent)
        temp=line[index:].find(')')
        index+=(temp+1)
        count+=1
        if temp==-1:
            break
    return(detected_intents)


def train(file_name,out_dir,no_warning=False):
    global nlu_config


    ####Seperating lines with entities from that of lines without entities, for keyword training
    try:
        file_content='\n'.join([i for i in open(file_name,'r').read().split('\n') if(i!='' and ('(' in i or "#" in i ) )])
    except FileNotFoundError :
        raise FileNotFoundError("Kindly pass the correct name of the md file")

    try:
        if not(isdir(out_dir)):
            mkdir(out_dir)
        elif not(no_warning) :
            choice=input("The desired directory name for training already exists .. \n proceeding may erase the contents should we proceed(y/n): ").lower()
            if 'n' in choice :
                raise UserInterruptError('Program execution cancelled')
            else:
                rmdir(out_dir)
                mkdir(out_dir)
        else:
            rmdir(out_dir)
            mkdir(out_dir)
        mkdir(out_dir+'/training_data')
        file_nlu=open(out_dir+'/training_data/nlu.md','w')
        file_nlu.write('## intent:debug\n')
    except FileNotFoundError :
        raise FileNotFoundError('Invalid output directory name passed!!')
    except PermissionError :
        raise PermissionError("File permissions not satisfied for the operation")
    entity_intents={} #Holds data aobut which entity belongs to whk.ich intent
    intents=[]
    entities_parsed=[]
    current_intent=None

    intents_split=[i for i in file_content.split('##')  if i!=''  ]###Removing unnesseaccary \n (blank lines)

    for i in range(len(intents_split)) :
        intent_name=intents_split[i].split('\n')[0].split(':')[1].strip()
        entities_detected=extract_entities(intents_split[i])
        intents.append(intent_name)
        for j in range(len(entities_detected)):
            if entities_detected[j] in entities_parsed :
                choice=entities_detected[j]+'"'
                while choice in entities_parsed :
                    choice=choice+'"'
                intents_split[i]=intents_split[i].replace('('+entities_detected[j]+')','('+choice+')')
                entities_detected[j]=choice
                print("Common entity detected fixing...")
            entity_intents[entities_detected[j]]=i #Storing the entity names under the intent
            entities_parsed.append(entities_detected[j])
        print('%d out of %d'%(i,len(intents_split)))#Saving the intents parsed for further tracking
    intents_split=['']+intents_split
    file_nlu.write('##'.join(intents_split))
    rasa_data=(intents,entity_intents)
    file_nlu.close()
    open(out_dir+'/nlu_config.yml','w').write(nlu_config)
    feather_file=open(out_dir+'/feather.data','wb')
    pickle.dump(rasa_data,feather_file)

    ####Seperating lines without entities from lines with entities for further fuzzy wuzzy learning

    file_lines=[i for i in open(file_name,'r').read().split('\n') if (i!='' and ("#" in i or '(' not in i ))  ]
    fuzzy_wuzzy={}
    intents=[]
    count=-1

    for i in file_lines :
        if "#" in i:
            count+=1
            intents.append(i.split(':')[1].strip())
        else:
            fuzzy_wuzzy[i[i.find('-')+1:]]=count

    fuzzy_data=(intents,fuzzy_wuzzy)
    pickle.dump(fuzzy_data,feather_file)
    feather_file.close()
    if version[0]=='3':
        system("python3 -m rasa_nlu.train -c %s/nlu_config.yml --data %s/training_data/nlu.md -o %s/models --fixed_model_name nlu --project current --verbose "%(out_dir,out_dir,out_dir))
    else:
         system("python3 -m rasa_nlu.train -c %s/nlu_config.yml --data %s/training_data/nlu.md -o %s/models --fixed_model_name nlu --project current --verbose "%(out_dir,out_dir,out_dir))

class CorruptedModelError(Exception):
    pass

class feather_interpreter():

    def __init__(self,dirname):

        if not(isdir(dirname)):
            raise FileNotFoundError('Pass a valid dirname')

        try:
            self.__rasa_engine=Interpreter.load(dirname+'/models/current/nlu')
        except rasa_model_error :
            raise CorruptedModelError("Sorry the contents of the model file is changed or corrupted , try training again")

        try:
            feather_file=open(dirname+"/feather.data",'rb')
            self.rasa_data=pickle.load(feather_file)
            self.fuzzy_data=pickle.load(feather_file)
            feather_file.close()
        except FileNotFoundError :
            raise CorruptedModelError("Sorry the contents of the model file is changed or corrupted , try training again")

    def __clean_entity(self,inp_entity):
        out_str=""
        for i in inp_entity:
            if i=='"':
                return(out_str)
            out_str+=i
        return(out_str)

    def parse(self,inp_str):
        rasa_resp=self.__rasa_engine.parse(inp_str)
        feather_resp={'intent':{'value':'','confidence':0.0},'entities':[]}

        if len(rasa_resp['entities'])!=0:
            intent_confidence=0

            index=self.rasa_data[ 1 ][ (rasa_resp['entities'][0]['entity']) ]

            for i in range(len(rasa_resp['entities'])):
                rasa_resp['entities'][i]['entity']=self.__clean_entity(rasa_resp['entities'][i]['entity'])
                intent_confidence+=rasa_resp['entities'][i]['confidence']
            intent_confidence/=len(rasa_resp['entities'])

            feather_resp['intent']={'value':self.rasa_data[0][index],'confidence':intent_confidence}
            feather_resp['entities']=rasa_resp['entities']
        else:
            phrases=self.fuzzy_data[1].keys()
            best_match,confidence=process.extract(inp_str,phrases)[0]
            print(best_match)
            index=self.fuzzy_data[1][best_match]
            intent=self.fuzzy_data[0][index]
            feather_resp['intent']={'value':intent,'confidence':confidence}
        return(feather_resp)

if __name__=="__main__":
    train('nlu.md',out_dir='sample')
    feather_engine=feather_interpreter('sample')
    pprint(feather_engine.parse(input("Enter the input query:")))