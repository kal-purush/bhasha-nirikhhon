### standard
import streamlit as st
### Standard
import os
import shutil


tuneDictList=[{'name':"RFP",
               'description':"Inspired by the utter classic [Beneath a Steel Sky](https://en.wikipedia.org/wiki/Beneath_a_Steel_Sky). :floppy_disk: :floppy_disk: :floppy_disk:",
               'tunes':[{'match':"Loopy",'alias':"Loopy"},
                        {'match':"Sunday",'alias':"Sunday"},
                        {'match':"Interlude_VI ",'alias':"DiskChangeVI"},
                        {'match':"Uked",'alias':"Uked"},
                        {'match':"Soured ",'alias':"Soured"},
                        {'match':"Interlude_II ",'alias':"DiskChangeII"},
                        {'match':"HeavyHarp",'alias':"HeavyHarp"},
                        {'match':"Interlude_IV ",'alias':"DiskChangeIV"},
                        {'match':"drum_et_bass",'alias':"DrumEtBass"},
                        {'match':"alarms",'alias':"Alarms"},
                        {'match':"Interlude_V ",'alias':"DiskChangeV"},
                        {'match':"buzzerLong",'alias':"BuzzerLong"},
                        {'match':"holdChord",'alias':"HoldChord"},
                        {'match':"drumsShort",'alias':"DrumsShort"},
                        {'match':"trainer",'alias':"Trainer"},
                        {'match':"Interlude_I ",'alias':"DiskChangeI"},
                        {'match':"fromHere",'alias':"FromHere"},
                        {'match':"Interlude_III ",'alias':"DiskChangeIII"}
                        ]},
              {'name':"CopShow",
               'description':"Grim endings for grim stories. :smoking: :gun: :person_frowning:",
               'tunes':[{'match':"ender",'alias':"Ender"},
                        {'match':"outro",'alias':"Outro"},
                        {'match':"vaguely",'alias':"Vaguely"},
                        {'match':"ruphh",'alias':"Ruphh"}
                        ]},
              {'name':"Fuked",
               'description':"Uke based. :hear_no_evil:",
               'tunes':[{'match':"flutter",'alias':"Flutter"},
                        {'match':"observer",'alias':"Observer"},
                        {'match':"punchet",'alias':"Punchet"},
                        {'match':"tenoruke",'alias':"TenorUke"},
                        {'match':"somethingElse",'alias':"SomethingElse"},
                        {'match':"gripes",'alias':"Gripes"}
                        ]},
              {'name':"Between",
               'description':"Not properly categoreyest. :bust_in_silhouette:",
               'tunes':[{'match':"projected",'alias':"Projected"},
                        {'match':"returned",'alias':"Returned"},
                        {'match':"hapi",'alias':"Hapi"},
                        {'match':"noise",'alias':"Noise"},
                        {'match':"eudaimonia",'alias':"Eudaimonia"}
                        ]},
              {'name':"Covers",
               'description':"Bespoke ruinings of actual tunes. :scream_cat:",
               'tunes':[{'match':"CoS",'alias':"CoS"},
                        {'match':"K&J",'alias':"KandJ"},
                        {'match':"OYaz",'alias':"OYaz"},
                        {'match':"allmylittle",'alias':"AllMyLittle"}
                        ]}
             ]

def GetTuneListThing(inStr,myKey):
    my_item = next((item for item in tuneDictList if item['name'] == inStr), None)
    try:
        return my_item[myKey]
    except KeyError:
        return None
    return None

def FilterFiles(tuneDir, tuneDictList, suf=".mp3"):
    retList=[]
    for t in tuneList:
        try:
            for file in os.listdir(tuneDir):
            #st.write(os.path.join(pageDict['tuneDir'], file))
                if file.endswith(suf):
                    name=os.path.join(tuneDir, file)
                    short=name.split('/')[-1].replace(suf,'')
                    if t['match'].lower() in short.lower():
                        retList.append({'short':t['alias'],'name':name})
                        break
        except FileNotFoundError:
            st.write("No fileDir")
            return None
    return retList

def CopyFiles(tuneDir, tuneList, suf=".mp3"):

    for td in tuneDictList:
        for e,t in enumerate(td['tunes'],1):
            try:
                for file in os.listdir(tuneDir):
                #st.write(os.path.join(pageDict['tuneDir'], file))
                    if file.endswith(suf):
                        name=os.path.join(tuneDir, file)
                        short=name.split('/')[-1].replace(suf,'')
                        if t['match'].lower() in short.lower():
                            #shutil.copy(src,dst)
                            newName=name.replace(short,'/'+td['name']+'/'+('%02d' % e)+'_'+t['alias'])
                            if not os.path.isdir(tuneDir+'/'+td['name']):
                                os.mkdir(tuneDir+'/'+td['name'])
                            shutil.copy(name,newName)
                            break
            except FileNotFoundError:
                print("No fileDir")
    return None

#> cp /Users/kwraight/OneDrive\ -\ University\ of\ Glasgow/GarageBand/mp3_versions/* ~/tempTunes/
# CopyFiles("/Users/kwraight/tempTunes/", tuneDictList)