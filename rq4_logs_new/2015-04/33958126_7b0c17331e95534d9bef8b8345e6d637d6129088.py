# gitabel
# the world's smallest project management tool
# reports relabelling times in github (time in seconds since epoch)
# thanks to dr parnin
# todo:
# - ensure events sorted by time
# - add issue id
# - add person handle
 
"""
You will need to add your authorization token in the code.
Here is how you do it.
1) In terminal run the following command
curl -i -u <your_username> -d '{"scopes": ["repo", "user"], "note": "OpenSciences"}' https://api.github.com/authorizations
2) Enter ur password on prompt. You will get a JSON response.
In that response there will be a key called "token" .
Copy the value for that key and paste it on line marked "token" in the attached source code.
3) Run the python file.
python gitable.py
"""
from __future__ import print_function
import urllib2
import json
import re,datetime
import sys
class L():
    "Anonymous container"
    def __init__(i,**fields) :
        i.override(fields)
    def override(i,d): i.__dict__.update(d); return i
    def __repr__(i):
        d = i.__dict__
        name = i.__class__.__name__
        return name+'{'+' '.join([':%s %s' % (k,pretty(d[k])) for k in i.show()])+ '}'
    def show(i):
        lst = [str(k)+" : "+str(v) for k,v in i.__dict__.iteritems() if v != None]
        return ',\t'.join(map(str,lst))

#feature10:Percentage of issues using milestone
issueUseMilestone=0

def secs(d0):
    d = datetime.datetime(*map(int, re.split('[^\d]', d0)[:-1]))
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = d - epoch
    return delta.total_seconds()

def dump1(u,issues):
    token = "your token" # <===
    request = urllib2.Request(u, headers={"Authorization" : "token "+token})
    v = urllib2.urlopen(request).read()
    w = json.loads(v)
    if not w: return False
    for event in w:
        issue_id = event['issue']['number']
        if not event.get('label'): continue
        created_at = secs(event['created_at'])
        action = event['event']
        label_name = event['label']['name']
        user = event['actor']['login']
        milestone = event['issue']['milestone']
        if milestone != None:
            milestone = milestone['title']
            global issueUseMilestone
            issueUseMilestone += 1
        eventObj = L(when=created_at,
                    action = action,
                    what = label_name,
                    user = user,
                    milestone = milestone)

        all_events = issues.get(issue_id)
        if not all_events: all_events = []
        all_events.append(eventObj)
        issues[issue_id] = all_events
    return True
 
def dump(u,issues):
    try:
        return dump1(u, issues)
    except Exception as e:
        print(e)
        print("Contact TA")
    return False
 
def launchDump():
    page = 1
    issues = dict()
    #feature1: Number of issues
    numofIssues=0
    #feature2: Number of different labels
    numoflabels=0
    #Number of times each label was used
    labelnum=dict()
    
    f=open("Group6.txt","w")
    #issues2=dict()
    while(True):
        doNext = dump('https://api.github.com/repos/SuperCh-SE-NCSU/ProjectScraping/issues/events?page=' + str(page), issues)
        #print("page" + str(page))
        page += 1
        if not doNext : break
    for issue, events in issues.iteritems():
        print("ISSUE " + str(issue)+"\n")
        f.write("ISSUE "+str(issue)+"\n")
        numofIssues=issue
        for event in events:
            #k->key; v->value
            for k,v in event.__dict__.iteritems():
                if str(k) is 'what':
                    if str(v) in labelnum.keys():
                        labelnum[str(v)]=labelnum[str(v)]+1
                    else:
                        labelnum[str(v)]=1
            f.write(event.show()+"\n")
            f.write('\n')
            print(event.show())
            print('')
    f.close()        
    print ('==============Statistics==================')
    numoflabels=len(labelnum)
    print('Num of issues:',numofIssues)
    print('Num of labels:',numoflabels)
    global issueUseMilestone
    print('Issues using milestone', issueUseMilestone)
    for key, elem in labelnum.items():
        print(key, elem)        

    
        #issues2[str(issue)]=events
    #with open('ProjectScraping.json','wb') as fp:
    #    json.dump(issues2,fp)
    

    
launchDump() 