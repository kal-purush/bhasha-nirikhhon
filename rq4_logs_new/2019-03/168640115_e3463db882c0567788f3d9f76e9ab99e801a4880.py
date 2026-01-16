#package required to make api calls
import requests, base64, json, sys

#packages required for handling large data
import pandas as pd

#package imoported to implement sleep function
import time

#package for regex
import re

#package to covert string into list
import ast

#package required to suppress warnings about not having SSL certificate verification
from requests.packages.urllib3.exceptions import InsecureRequestWarning
#command to suppress warning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

f = open("input.txt", "r")
y=[]
for x in f:
  y.append(x.split("-"))

debug=y[9][1].strip()
if debug.lower()=="yes":
    debug=1
else:
    debug=0
ise_ip=y[0][1].strip()
if debug==1:
    print(ise_ip)
ise_un=y[1][1].strip()
if debug==1:
    print(ise_un)
ise_pw=y[2][1].strip()
if debug==1:
    print(ise_pw)
dnac_ip=y[4][1].strip()
if debug==1:
    print(dnac_ip)
dnac_un=y[5][1].strip()
if debug==1:
    print(dnac_un)
dnac_pw=y[6][1].strip()
if debug==1:
    print(dnac_pw)
   

#encoding for the request to authorize access to the DNAC instance
def encoder(un,pw):
    encodedvalue=un+":"+pw
    b64Val = base64.b64encode(encodedvalue.encode('UTF-8')).decode('utf-8')
    return (b64Val)

#to get token
def auth(ip,b64Val):
    try:
        r=requests.get('https://'+ ip +'/api/system/v1/auth/login', headers={"Authorization": "Basic %s" % b64Val,"Content-Type": "application/json"}, verify=False)
        #print(r.status_code)
        #checking for correct username and password
        if r.text!="success":
            print("Please check the data you entered again")
            return("error")
        #error handling 
        r.raise_for_status()
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
        sys.exit(1)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
        sys.exit(1)
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print ("Oops: Something Else",err)
        sys.exit(1) 
    
    #to extract the cookie
    a=r.headers['Set-Cookie'].split(";")
    b=a[0].split("=")
    c=b[1]
    cookie = {'X-JWT-ACCESS-TOKEN':c}
    #print(cookie)
    return(cookie)

#to get task id
def poller(ip,cookie,body):
    try:
        r= requests.post('https://'+ip+'/api/v1/network-device-poller/cli/read-request',cookies=cookie,json=body ,verify=False)
        if debug==1:
            print("POLL:",r.text,"\n")
        #checking if poller request was not successful
        pattern = re.compile('errorCode')
        match=pattern.findall(r.text)
        if match:
            return("error")
        #error handling
        r.raise_for_status()
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
        sys.exit(1)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
        sys.exit(1)
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print ("Oops: Something Else",err)
        sys.exit(1)
    
    #to extract task id
    pattern = re.compile(r'taskId":"([a-zA-z0-9-]+)"')
    match=pattern.findall(r.text)
    task_id = match[0]
    return(task_id)

#to get file id
def task(ip,cookie,task_id):
    try:
        r= requests.get('https://'+ip+'/api/v1/task/'+task_id,cookies=cookie,verify=False)
        if debug==1:
            print("\n TASK:" ,r.text)     
        #to extract file-id
        pattern=re.compile(r'fileId\\":\\"([a-zA-Z0-9-]+)\\')
        match=pattern.findall(r.text) 
        #when proper file-id is not generated return error
        if not match:
            return("error")
        #error handling
        r.raise_for_status()
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
        sys.exit(1)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
        sys.exit(1)
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print ("Oops: Something Else",err)
        sys.exit(1)
   
    #return file-id
    file_id=match[0]
    #print("FILEID: ",file_id,"\n")
    return (file_id)

#Function to get pac and sxp info
def fileo(ip,cookie,file_id):
    try:
        r= requests.get('https://'+ip+'/api/v1/file/%s' %file_id,cookies=cookie,verify=False)
        #print(repr(r.text))
        #error handling
        r.raise_for_status()
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
        sys.exit(1)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
        sys.exit(1)
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print ("Oops: Something Else",err)
        sys.exit(1)
        
    return r.text

#to get vrfnames
def vrf(ip,cookie,file_id):
    try:
        r= requests.get('https://'+ip+'/api/v1/file/%s' %file_id,cookies=cookie,verify=False)
        json_data = json.loads(r.text)
        #print(repr(r.text))
        #error handling
        r.raise_for_status()
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
        sys.exit(1)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
        sys.exit(1)
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print ("Oops: Something Else",err)
        sys.exit(1)
    
    #Extracting vrf names
    if not (json_data[0]['commandResponses']['SUCCESS']):
        return ("error")
    output = json_data[0]['commandResponses']['SUCCESS']['sh vrf']
    if debug==1:
        print("VRFRESPONSE: ",repr(output),"\n")
    vrfnames = re.findall("\n\s\s([\w+-]+)\s+", output, re.M | re.I)
    #to remove 'name' from vrfnames list
    vrfnames = vrfnames[1:]
    if debug==1:
        print("\n VRF: ",vrfnames,"\n")
    return vrfnames

#to get sgts(local group tags LTGs) and iplist
def sgtlist(vrfnames,ip,cookie,devid,unknwnval):
    responses=[]

    for element in vrfnames:
        body = {"commands": ["sh cts role-based sgt-map vrf %s all" %element],
                "description": "string",
                "deviceUuids": [devid],
                "name": "string",
                "timeout": 0 }
        task_id = poller(ip,cookie,body)
        #print(task_id)
        time.sleep(2)
        file_id = task(ip,cookie,task_id)
        #print(file_id)
        while file_id=="error":
            time.sleep(2)
            task_id = poller(ip,cookie,body)
            time.sleep(2)
            file_id = task(ip,cookie,task_id)

        try:
            r= requests.get('https://'+ip+'/api/v1/file/%s' %file_id,cookies=cookie,verify=False)
            #print(repr(r.text))
            #error handling
            r.raise_for_status()
        except requests.exceptions.Timeout as errt:
            print ("Timeout Error:",errt)
            sys.exit(1)
        except requests.exceptions.ConnectionError as errc:
            print ("Error Connecting:",errc)
            sys.exit(1)
        except requests.exceptions.HTTPError as errh:
            print ("Http Error:",errh)
            sys.exit(1)
        except requests.exceptions.RequestException as err:
            print ("Oops: Something Else",err)
            sys.exit(1)

        responses.append(r.text)
    
    #Global sgt response    
    body = {"commands": ["sh cts role-based sgt-map all"],
                "description": "string",
                "deviceUuids": [devid],
                "name": "string",
                "timeout": 0 }
    task_id = poller(ip,cookie,body)
    #print(task_id)
    time.sleep(2)
    file_id = task(ip,cookie,task_id)
    #print(file_id)
    while file_id=="error":
        time.sleep(2)
        task_id = poller(ip,cookie,body)
        time.sleep(2)
        file_id = task(ip,cookie,task_id)

    try:
        r= requests.get('https://'+ip+'/api/v1/file/%s' %file_id,cookies=cookie,verify=False)
        #print("SGTGLOBAL:",r.text)
        #error handling
        r.raise_for_status()
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
        sys.exit(1)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
        sys.exit(1)
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print ("Oops: Something Else",err)
        sys.exit(1)
    responses.append(r.text)
        
    #print("\n SGTRESP: ",responses)
    sgtlist=[] #list of sgt's
    sgtvrf=[] #list of sgt's to display in vrf dataframe
    iplist=[] #list of ip's
    
    #extracting sgt's and ip's
    for i in range(len(responses)):
        if debug==1:
            print("\n SGTRESP: ",responses[i])
        if((responses[i].find('Active IPv4'))==-1):
            sgtlist.append('')
            sgtvrf.append('')
            iplist.append('')
            continue
        sgt=[]
        ips=[]
        pattern=re.compile(r'\\n(\d*\.\d*\.\d*\.\d*/?\d*?)\s+(\d+)')
        match=pattern.findall(responses[i])
        #print(match)
        for i in match:
            sgt.append(i[1])
            ips.append(i[0])
        sgtvrf.append(sgt)
        sgt = list(dict.fromkeys(sgt)) #to remove duplicate sgts
        #print(sgt)
        sgtlist.append(sgt)
        iplist.append(ips)
    if unknwnval == 1:
        sgtlist.append(['0']) #Unknown sgt is 0

    if debug==1:
        print ("SGT(LGT): ",sgtlist)
    return sgtlist, iplist, sgtvrf

#to get dgtlist, sgacllist and acllist
def sgtperm(sgtlist,ip,cookie,devid):
    dgtlist=[]
    sgacllist=[]
    acllist=[]
    for element in sgtlist:
        if not element:
            dgtlist.append('')
            sgacllist.append('')
            acllist.append('')
            continue
        dgt=[]
        sgacl=[]
        acl=[]
        for i in element:
            body = {"commands": ["sh cts role-based permission to %s" %i],
                    "description": "string",
                    "deviceUuids": [devid],
                    "name": "string",
                    "timeout": 0 }
            task_id = poller(ip,cookie,body)
            #print(task_id)
            time.sleep(2)
            file_id = task(ip,cookie,task_id)
            while file_id=="error":
                time.sleep(2)
                task_id = poller(ip,cookie,body)
                time.sleep(2)
                file_id = task(ip,cookie,task_id)
            #print(file_id)
            sgacl1=[]
            acl1=[]
            try:
                r= requests.get('https://'+ip+'/api/v1/file/%s' %file_id,cookies=cookie,verify=False)
                #print(repr(r.text))
                #error handling
                r.raise_for_status()
            except requests.exceptions.Timeout as errt:
                print ("Timeout Error:",errt)
                sys.exit(1)
            except requests.exceptions.ConnectionError as errc:
                print ("Error Connecting:",errc)
                sys.exit(1)
            except requests.exceptions.HTTPError as errh:
                print ("Http Error:",errh)
                sys.exit(1)
            except requests.exceptions.RequestException as err:
                print ("Oops: Something Else",err)
                sys.exit(1)
                
            #extracting dgt, sgacl and acl
            #print(repr(r.text))
            res = re.findall(r'IPv4 Role-based permissions from group ([a-zA-Z\d]+)',r.text) #extracting dgts
            for n, i in enumerate(res):
                if i == 'Unknown':
                    res[n] = '0'
            if debug==1:
                print(res)
            if not res:
                dgt.append('')
                sgacl.append('')
                acl.append('')
                continue
            dgt.append(res)
            res2 = (r.text).split("IPv4")
            res2.pop(0)
            sgacl1=[]
            acl1=[]
            for i in res2:
                #print(i)
                res3 = re.findall(r'\(configured\):',i) #skip if configured is found
                #print(res3)
                if res3:
                    continue
                res4 = re.findall(r'\\n\\t([\w\d -]+)',i) #exctracting acl
                #print(res4)
                res5 = re.findall(r'\\n\\t([\w ]+)',i) #extracting sgacl
                #print(res5)
                sgacl1.append(res5)
                acl1.append(res4)
            sgacl.append(sgacl1)
            acl.append(acl1)
            #print(dgt,sgacl,acl)
        dgtlist.append(dgt)
        sgacllist.append(sgacl)    
        acllist.append(acl)
    if debug==1:
        print("DGT(RGT): ",dgtlist,"\n SGACL: ", sgacllist, "\n ACL: ",acllist,"\n")        
    return dgtlist, sgacllist, acllist

#to extract aclcontent
def aclcontent(acllist,ip,cookie,devid):
    aclcontlist=[]
    body = {"commands": ['sh ip access-lists'],
            "description": "string",
            "deviceUuids": [devid],
            "name": "string",
            "timeout": 0 }
    task_id = poller(ip,cookie,body)
    #print(task_id)
    time.sleep(2)
    #print(type(file_id))
    file_id = task(ip,cookie,task_id)
    while file_id=="error":
        time.sleep(2)
        task_id = poller(ip,cookie,body)
        time.sleep(2)
        file_id = task(ip,cookie,task_id)
        #print(file_id)
    try:
        r= requests.get('https://'+ip+'/api/v1/file/%s' %file_id,cookies=cookie,verify=False)
        #json_data = json.loads(r.text)
        l=r.text.find('Role-based IP access list')
        x=r.text[l:]
        if debug==1:
            print("ACLRESP : " ,repr(x), "\n")
        #error handling
        r.raise_for_status()
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
        sys.exit(1)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
        sys.exit(1)
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print ("Oops: Something Else",err)
        sys.exit(1)
        
    #print(acllist) 
    z=x.split("\\n")
    aclcontlist=[]
    for el in acllist:
        #print("1",el)
        if not el:
            aclcontlist.append('')
            continue
        aclcont1=[]
        for el1 in el:
            #print("2",el1)
            if not el1:
                aclcont1.append('')
                continue
            aclcont=[]
            for el2 in el1:
                #print("3",el2)
                match=[]
                for el3 in el2:
                    #print("4",el3)
                    res1=[]
                    for i in z:
                        if el3 in i:
                            pos = z.index(i) 
                            for j in range(pos+1,len(z)):
                                if not z[j].startswith(' '):
                                    break
                                else:
                                    res = re.findall(r'\d+\s([a-z ]+)',z[j])
                                    #print(res)
                                    j=j+1
                                    res1.append(res[0].strip())
                    #print("0:",res1)
                    if len(res1)==1:
                        match.append(res1[0])
                    else:
                        match.append(res1)
                #print("3:",match)
                aclcont.append(match)
            #print("2:",aclcont)    
            aclcont1.append(aclcont)
        #print("1:",aclcont1)
        aclcontlist.append(aclcont1)
    if debug==1:
        print("ACLCONTENT:", aclcontlist)
    return aclcontlist

#to get ISE egressmatrix cell ids
def egressconfig(ip,b64Val):
    ids=[]
    repos=[]
    try:
        r= requests.get('https://'+ip+'/ers/config/egressmatrixcell?simple=yes&page=1',headers={"Authorization": "Basic %s" % b64Val,"Content-Type": "application/json","Accept": "application/json"}, verify=False)
        #print(r.text)
        #print(type(r.text))
        #print(r.status_code)
        #checking for correct username and password
        if r.status_code!=200:
            print("Please check the data you entered again")
            return("error")
        json_data = json.loads(r.text)
        repos.append(json_data)
        #checking for next page
        try:
            while json_data["SearchResult"]["nextPage"]['rel'] == 'next':
                url=json_data["SearchResult"]["nextPage"]['href']
                r=requests.get(url,headers={"Authorization": "Basic %s" % b64Val,"Content-Type": "application/json","Accept": "application/json"}, verify=False)
                #print(r.text)
                #print(r)
                json_data = json.loads(r.text)
                try:
                    x=json_data["SearchResult"]["nextPage"]
                    #print(x)
                except:
                    print("")
                repos.append(json_data)
        except:
            print("")
            #print(repos,len(repos))

        for i in repos:
            #print(i)
            x=i['SearchResult']['resources']
            #print(x)
            for element in range(len(x)):
                ids.append(x[element]['id'])    
        #print(ids)
        #Error handling 
        r.raise_for_status()
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
        sys.exit(1)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
        sys.exit(1)
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print ("Oops: Something Else",err)
        sys.exit(1)
    if debug==1:
        print(ids)
        print(len(ids))
    return(ids)

#to get egressmatrix cell responses 
def egressresp(ids,ip,b64Val):
    responses=[]
    for element in ids:
        try:
            r= requests.get('https://'+ip+'/ers/config/egressmatrixcell/'+element,headers={"Authorization": "Basic %s" % b64Val,"Content-Type": "application/json","Accept": "application/json"}, verify=False)
            json_data = json.loads(r.text)
            #print(element)
            responses.append(json_data)
            #error handling
            r.raise_for_status()
        except requests.exceptions.Timeout as errt:
            print ("Timeout Error:",errt)
            sys.exit(1)
        except requests.exceptions.ConnectionError as errc:
            print ("Error Connecting:",errc)
            sys.exit(1)
        except requests.exceptions.HTTPError as errh:
            print ("Http Error:",errh)
            sys.exit(1)
        except requests.exceptions.RequestException as err:
            print ("Oops: Something Else",err)
            sys.exit(1)
    #print(responses)
    return responses

#to get sgtid,dgtid,sgaclid
def idsresp(responses):
    sgtid=[]
    dgtid=[]
    sgaclid=[]
    for i in range(len(responses)):
        sgtid.append(responses[i]['EgressMatrixCell']['sourceSgtId'])
        dgtid.append(responses[i]['EgressMatrixCell']['destinationSgtId'])
        sgaclid.append(responses[i]['EgressMatrixCell']['sgacls'])   
    
    return sgtid,dgtid,sgaclid

#to get sgtresponse and dgtresponse
def gtresponse(ip,b64Val,gtid):
    gtresp=[]
    for i in range(len(gtid)):
        try:
            r= requests.get('https://'+ip+'/ers/config/sgt/'+gtid[i],headers={"Authorization": "Basic %s" % b64Val,"Content-Type": "application/json","Accept": "application/json"}, verify=False)
            #print(r.text)
            json_data = json.loads(r.text)
            gtresp.append(json_data)
            #error handling
            r.raise_for_status()
        except requests.exceptions.Timeout as errt:
            print ("Timeout Error:",errt)
            sys.exit(1)
        except requests.exceptions.ConnectionError as errc:
            print ("Error Connecting:",errc)
            sys.exit(1)
        except requests.exceptions.HTTPError as errh:
            print ("Http Error:",errh)
            sys.exit(1)
        except requests.exceptions.RequestException as err:
            print ("Oops: Something Else",err)
            sys.exit(1)
    #print(gtresp)
    return(gtresp) 

#to get sgacllist and aclcontentlist
def sgaclresponse(ip,b64Val,sgaclid):
    sgaclresp=[]
    for i in sgaclid:
        responses=[]
        for l in i:
            try:
                r= requests.get('https://'+ip+'/ers/config/sgacl/'+l,headers={"Authorization": "Basic %s" % b64Val,"Content-Type": "application/json","Accept": "application/json"}, verify=False)
                json_data = json.loads(r.text)
                responses.append(json_data)
                #error handling
                r.raise_for_status()
            except requests.exceptions.Timeout as errt:
                print ("Timeout Error:",errt)
                sys.exit(1)
            except requests.exceptions.ConnectionError as errc:
                print ("Error Connecting:",errc)
                sys.exit(1)
            except requests.exceptions.HTTPError as errh:
                print ("Http Error:",errh)
                sys.exit(1)
            except requests.exceptions.RequestException as err:
                print ("Oops: Something Else",err)
                sys.exit(1)
        
        
        sgaclresp.append(responses)
    sgaclvallist=[]
    aclcontlist=[]
    
    for i in sgaclresp:
        if debug==1:
            print (i)
        sgaclval=[]
        aclcont=[]
        for j in i:
            #print(j)
            sgaclval.append(j['Sgacl']['name'])
            x=j['Sgacl']['aclcontent']
            k=x.split("\n")
            if len(k)==1:
                for z in k:
                    aclcont.append(z)
            else:
                aclcont.append(k) 
            #print(sgaclval,aclcont) 
        sgaclvallist.append(sgaclval)
        aclcontlist.append(aclcont)
        #print(sgaclvallist)
        #print(aclcontlist)
    if debug==1:
        print("SGACL: ",sgaclvallist)
        print("ACLCONTENT: ",aclcontlist)
    return sgaclvallist,aclcontlist

#to get network devices information
def devices(ip,cookie):
    try:
        r= requests.get('https://'+ip+'/api/v1/network-device/count',cookies=cookie,verify=False)
        json_data=json.loads(r.text)
        count=json_data["response"]
        if debug==1:
            print("No of device: ",count)
        #error handling
        r.raise_for_status()
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
        sys.exit(1)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
        sys.exit(1)
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print ("Oops: Something Else",err)
        sys.exit(1)
    
    try:
        r= requests.get('https://'+ip+'/api/v1/network-device/',cookies=cookie,verify=False)
        #print(r.text)
        #error handling
        r.raise_for_status()
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
        sys.exit(1)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
        sys.exit(1)
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print ("Oops: Something Else",err)
        sys.exit(1)

    obj = json.loads(r.text)
    
    deviceid=[]
    device=[]
    mipaddr=[]
    devinfo=[]

    for i in range(count):
        x=obj['response'][i]
        if debug==1:
            print(x)
        esc=x["type"].find("Wireless Controllers")
        esc1=x["type"].find("Access Point")
        if x["errorCode"]=='DEV-UNREACHED' or esc!=-1 or esc1!=-1:
            continue
        deviceid.append(x["instanceUuid"])
        device.append(x["type"])
        mipaddr.append(x["managementIpAddress"])
        devinfo.append(x)
        if debug==1:
            print("in")
    return(deviceid, device, mipaddr, count, devinfo)

#to get default permissions    
defaultperm={}  #list to store default permissions
def default(devid,ip,cookie,count,defperm):
    body = {"commands": ['sh cts role-based permission default'],
                    "description": "string",
                    "deviceUuids": [devid],
                    "name": "string",
                    "timeout": 0 }
    task_id = poller(ip,cookie,body)
    #print(task_id)
    time.sleep(2)
    file_id = task(ip,cookie,task_id)
    while file_id=="error":
        time.sleep(2)
        task_id = poller(ip,cookie,body)
        time.sleep(2)
        file_id = task(ip,cookie,task_id)
        #print(file_id)
    try:
        r= requests.get('https://'+ip+'/api/v1/file/%s' %file_id,cookies=cookie,verify=False)
        if debug==1:
            print("\n DEFAULT RESP:" , r.text)
        #error handling
        r.raise_for_status()
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
        sys.exit(1)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
        sys.exit(1)
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print ("Oops: Something Else",err)
        sys.exit(1)
    
    #print(r.text)
    res = re.findall(r'IPv4 Role-based permissions default:\\n\\t([a-zA-z _]+)-..',r.text)
    #print(res)
    if res==defperm:
        defaultperm.update({"device%s"%count:str(res)+" matches"})
    else:
        defaultperm.update({"device%s"%count:str(res)+" does not match"})
    
    return(defaultperm)
    
#============================================ISE=========================================================
success=0
unknwnval=0

print("Reading ISE Response")

while(success==0):
    #un = input("Enter the username of the ISE instance here: ")
    #pw = input("Enter the password of the ISE instance here: ")
    #ip = input("Enter the ***correct ip address of the ISE instance here: ")
    un = ise_un # enter the username of the ISE instance here
    pw = ise_pw # enter the password of the ISE instance here
    ip = ise_ip # enter the ip address of the ISE instance here

    #encoding for the request to authorize acces to the ISE instance
    b64Val = encoder(un,pw)
    #print(b64Val)

    ids=[]
    #Calls https://ip/ers/config/egressmatrixcell and returns all the id's
    ids = egressconfig(ip,b64Val)
    if ids!="error":
       success=1

responses=[]
#Calls https://ip/ers/config/egressmatrixcell/id and returns all the matrix cell responses
responses = egressresp(ids,ip,b64Val)

sgtid=[]
dgtid=[]
sgaclid=[]
#Extracts sgt-id, dgt-id, and sgacl-id
sgtid,dgtid,sgaclid = idsresp(responses)
#print(len(ids))
#print(len(responses))
#print(len(sgtid), len(dgtid), len(sgaclid))

sgtresp=[]
#Calls https://ip/ers/config/sgt/sgtid to get sgt response
sgtresp = gtresponse(ip,b64Val,sgtid)

dgtresp=[]
#Calls https://ip/ers/config/sgt/dgtid to get sgt response
dgtresp = gtresponse(ip,b64Val,dgtid)

#print(len(sgtresp),len(dgtresp))

#extracting DGT's
dgtval=[]
for i in range(len(dgtresp)):
    dgtval.append(dgtresp[i]['Sgt']['value'])

#extracting SGT's
sgtval=[]
for i in range(len(sgtresp)):
    sgtval.append(sgtresp[i]['Sgt']['value'])
#print(sgtval,dgtval)
#print(len(sgtval),len(dgtval))    

sgaclval=[]
aclcont =[]
#Calls https://ip/ers/config/sgacl/sgaclid and returns sgacl and aclcontent
sgaclval, aclcont= sgaclresponse(ip,b64Val,sgaclid)
#print(len(sgaclval),len(aclcont))
#print(sgaclval)
#print(aclcont)

print("generating ISE dataframe")

#Create pandas dataframe for ISE and csv file apitable.csv
pd.set_option('display.max_colwidth', -1) #to display complete data frame
df=pd.DataFrame([responses[0]['EgressMatrixCell']])
for i in range(1,len(responses)):
    df1=pd.DataFrame([responses[i]['EgressMatrixCell']])
    df=pd.concat([df,df1],sort='FALSE',ignore_index='TRUE')    
    
name=[]
defaultRule=[]
for i in range(0,len(responses)):
    if debug==1:
        print(responses[i])
    name.append(responses[i]['EgressMatrixCell']['name'])
    defaultRule.append(responses[i]['EgressMatrixCell']['defaultRule'])
    print(responses[i]['EgressMatrixCell']['defaultRule'])
      
#Removing columns that are not necessary
del df['description']
del df['link']
del df['matrixCellStatus']
    
#renaming columns
df.columns = ['defaultRule','destinationSgtId','id','name','sgacls-id','sourceSgtId']

#Removing columns that are not necessary
del df['destinationSgtId']
del df['id']
del df['sgacls-id']
del df['sourceSgtId']

#adding sgtval column
df['sgtval']=sgtval
    
#adding dgtval column
df['dgtval']=dgtval

#print(df)

df1=df

#function to flatten lists
def flat(nums):
    res = []
    index = []
    for i in range(len(nums)):
        if isinstance(nums[i], list):
            res.extend(nums[i])
            index.extend([i]*len(nums[i]))
        else:
            res.append(nums[i])
            index.append(i)
    return res,index

x="sgaclval"
list_flat,list_index=flat(eval(x))
#print(list_flat,list_index)
dataframe = pd.DataFrame({x:list_flat},index=list_index)
df1 = pd.concat([df1,dataframe],axis=1,sort=False)

x="aclcont"
list_flat,list_index=flat(eval(x))
#print(list_flat,list_index)
dataframe = pd.DataFrame({x:list_flat},index=list_index)
df1 = pd.concat([df1,dataframe],axis=1,sort=False)

df1.columns=["defaultRule","name","sgtval","dgtval","sgacl","aclcont"]
#print(df1)
df1['aclcont'] = df1['aclcont'].astype(str)
df1.drop_duplicates(keep="first",inplace=True)
#print(df1)

df['sgaclval']=sgaclval
df['aclcont']=aclcont

#inorder to drop duplicates in dataframe lists have to converted to string for conversion
df['sgaclval'] = df['sgaclval'].astype(str)
df['aclcont'] = df['aclcont'].astype(str)
df.drop_duplicates(keep="first",inplace=True)
#print(df)

if debug==1:
    print(name, defaultRule, sgtval, dgtval, sgaclval, aclcont)
final_list = []
for b, c, d, e, f, g in zip(defaultRule, name, sgtval, dgtval, sgaclval, aclcont):
    #print(b, c, d, e, f, g)
    if any([f, g]):
        for f1, g1 in zip(f, g):
            #print(f1, g1)
            if(isinstance(g1, str)):
                final_list.append([b, c, d, e, f1, g1])
            else:
                for g2 in g1:
                    final_list.append([b, c, d, e, f1, g2])
                    
df2 = pd.DataFrame(final_list, columns=["defaultRule","name","sgtval","dgtval","sgacl","aclcont"]) 
df2.drop_duplicates(keep="first",inplace=True)

if debug==1:
    print("ISE")
    print(df2)

df2.to_csv('ise.csv')

j=-1
for i,k in zip(df['name'],df['dgtval']):
    j=j+1
    if i=="ANY-ANY": #if name "ANY-ANY" if found in ISE table check if default permissions of ISE and network devices match
        defaultval = 1
        defperm = df['sgaclval'][j]
        defperm = ast.literal_eval(defperm)
    if k==0: #if DGT 0 is found in ISE table append 0 to sgtlist(lgt) of each network device 
        unknwnval = 1

#===============================================DNAC=============================================================
        
success=0

print("Reading DNAC response")

while(success==0):
    #un = input("Enter the username of the DNAC instance here: ")
    #pw = input("Enter the password of the DNAC instance here: ")
    #ip = input("Enter the ip address of the DNAC instance here: ")
    
    un = dnac_un # enter the username of the DNAC instance here
    pw = dnac_pw # enter the password of the DNAC instance here
    ip = dnac_ip # enter the ip address of the DNAC instance here

    #encoding for the request to authorize acces to the DNAC instance
    b64Val = encoder(un,pw)

    #Calls https://ip/api/system/v1/auth/login and returns token
    cookie = auth(ip, b64Val)
    if cookie!="error":
        success=1


#Calls https://ip/api/v1/network-device/count and https://ip/api/v1/network-device to return device-ids' , device names, management IP addresses, no. of devices and device informations
deviceid, devicenames, mipaddr, count, devinfo = devices(ip,cookie)
#print(deviceid, devicenames, mipaddr, count)

count=-1 #Variable to keep track of no of devices
dfa={} #dictionary of data frame of each device
dfa1={}
dfa2={}
dfa3={}
exist=[] #to keep track of which device has a dataframe
pacinfo={} #PAC Information
sxpinfo={} #SXP Connections Information
sgtbig={}  #list of sgts(LGT) of all devices
dgtbig={}  #list of dgts(RGT) of all devices
DEVICES={} #to store device information

for devid in deviceid:
    count=count+1 #Counter for no of devices
    if debug==1:
        print("\n",count," DEVICE:",devid," : ",devicenames[count],"\n")
    
    print("Reading device ",count)
    
    #To get Pac info and SXP info
    body = {"commands": ["show cts sxp connections","show cts pac"],
            "description": "string",
            "deviceUuids": [devid],
            "name": "string",
            "timeout": 0 }
     
    #https://ip/api/v1/network-device-poller/cli/read-request  and returns task-id
    task_id = poller(ip,cookie,body)
    
    #Giving some time so that dnac returns proper file-id. 
    time.sleep(2)

    #Calls https://ip/api/v1/task/task-id and returns file-id
    file_id = task(ip,cookie,task_id)
    while file_id=="error":
        time.sleep(2)
        task_id = poller(ip,cookie,body)
        time.sleep(2)
        file_id = task(ip,cookie,task_id)
        
    #Calls https://ip/api/v1/file/file-id and returns response from which pac info and SXP info has to be parsed
    output=fileo(ip,cookie,file_id)
    #print("PAC AND SXP", output)
    
    #to extract pac info
    l=output.find('Credential Lifetime')
    if l==-1:
        pacinfo.update({"device%s"%count:"No Information found"})
    else:    
        #print(output)
        output=output[l:]
        out1=output.split("\\n")
        pacinfo.update({"device%s"%count:out1[0]})
    
    #to extract SXP info
    pattern = re.compile(r'\\n\s(SXP)\s+:\s(\w+)\\n')
    match=pattern.findall(output)
    #print(match)
    for el in match:
        sxp="{0} : {1}".format(el[0],el[1])
    sxpinfo.update({"device%s"%count:sxp})
    
    if debug==1:
        print(pacinfo)
        print(sxpinfo)
    
    #check default permissions
    if defaultval==1:
        defaultperm=default(devid,ip,cookie,count,defperm)
    
    if debug==1:
        print(defaultperm)
    
    body = {"commands": ["sh vrf"],
            "description": "string",
            "deviceUuids": [devid],
            "name": "string",
            "timeout": 0 }

    #https://ip/api/v1/network-device-poller/cli/read-request  and returns task-id
    task_id = poller(ip,cookie,body)
    
    #Giving some time so that dnac returns proper file-id output
    time.sleep(2)

    #Calls https://ip/api/v1/task/task-id and returns file-id
    file_id = task(ip,cookie,task_id)
    while file_id=="error":
        time.sleep(2)
        task_id = poller(ip,cookie,body)
        time.sleep(2)
        file_id = task(ip,cookie,task_id)

    #Calls https://ip/api/v1/file/file-id and we are extracting vrfnames from the response
    vrfnames = vrf(ip,cookie,file_id)
    
    #returns the sgt-list for every vrfname
    sgtlst, iplist, sgtvrf = sgtlist(vrfnames,ip,cookie,devid,unknwnval)
    
    dgtlst, sgacllst, aclcontlst = sgtperm(sgtlst,ip,cookie,devid)
    
    #function to check if a list is empty
    def isListEmpty(inList):
        return inList == '' or isinstance(inList, list) and (not inList or all(map(isListEmpty, inList)))
    
    #print(isListEmpty(dgtlst))
    if (isListEmpty(dgtlst)):#skip if dgt list is empty
        if debug==1:
            print("dgt list is emptyy")
        dfa2.update({"device%s"%count:''})
        dfa1.update({"device%s"%count:''})
        continue
   
    #returns aclcont list
    acl = aclcontent(aclcontlst,ip,cookie,devid)
    #print(acl)
    
    exist.append(count)
    
    DEVICES.update({"device%s"%count:devinfo[count]})
    
    print("generating dataframe for device ",count)
    
    if debug==1:
        print("FINAL",sgtlst,"\n",dgtlst,"\n",sgacllst,"\n",acl)
    
    pd.set_option('display.max_colwidth', -1)
    final_list = []
    for b, c, d, e in zip(sgtlst, dgtlst, sgacllst, acl):
        if any([b, c, d, e]):
            for b1, c1, d1, e1 in zip(b, c, d, e):
                if any([c1,d1,e1]):
                    for c2, d2, e2 in zip(c1, d1, e1):
                        for d3, e3 in zip(d2, e2):
                            #print(repr(type(e3)))
                            if(isinstance(e3, str)):
                                final_list.append([b1, c2, d3, e3])
                            else:
                                for e4 in e3:
                                    final_list.append([b1, c2, d3, e4])
                else:
                    final_list.append([b1, c1, d1, e1])
        else:
            final_list.append([b, c, d, e])

    dfa2["device%s"%count] = pd.DataFrame(final_list, columns=['LGT', 'RGT', 'SGACL', 'ACLCONT'])
    dfa2["device%s"%count].drop_duplicates(keep="first",inplace=True)
    
    #print(dfa2)
    
    final_list = []
    for b, c, d, e in zip(sgtlst, dgtlst, sgacllst, acl):
        #print("1",b,c,d,e)
        if any([b, c, d, e]):
            for b1, c1, d1, e1 in zip(b, c, d, e):
                #print("2",c1,d1,e1)
                if any([c1,d1,e1]):
                    for c2, d2, e2 in zip(c1, d1, e1):
                        #print("3",c2,d2,e2)
                        for d3, e3 in zip(d2, e2):
                            final_list.append([b1, c2, d3, e3])
                else:
                    final_list.append([b1, c1, d1, e1])
        else:
            final_list.append([b, c, d, e])

    dfa3["device%s"%count] = pd.DataFrame(final_list, columns=['LGT', 'RGT', 'SGACL', 'ACLCONT'])
    dfa3["device%s"%count]['ACLCONT'] = dfa3["device%s"%count]['ACLCONT'].astype(str)
    dfa3["device%s"%count].drop_duplicates(keep="first",inplace=True)
    #print(dfa3["device%s"%count])

    if unknwnval==1:
            vrfnames.append('')

    #Create dataframe for all the data extracted from dnac
    dfa["device%s"%count]=pd.DataFrame({"VRFNames":vrfnames})

    x="sgtlst"
    list_flat,list_index=flat(eval(x))
    dataframe = pd.DataFrame({x:list_flat},index=list_index)
    dfa["device%s"%count] = pd.concat([dfa["device%s"%count]['VRFNames'],dataframe],axis=1,sort=False)
    sgtbig.update({"device%s"%count:list_flat})
    
    x="dgtlst"
    list_flat,list_index=flat(eval(x))
    #print(list_flat,list_index)
    dataframe = pd.DataFrame({x:list_flat},index=list_index)
    dfa["device%s"%count] = pd.concat([dfa["device%s"%count],dataframe],axis=1,sort=False)
    dgtbig.update({"device%s"%count:list_flat})
    
    x="sgacllst"
    list_flat,list_index=flat(eval(x))
    #print (list_flat,list_index)
    dataframe = pd.DataFrame({x:list_flat},index=list_index)
    dfa["device%s"%count] = pd.concat([dfa["device%s"%count],dataframe],axis=1,sort=False)
    
    x="acl"
    list_flat,list_index=flat(eval(x))
    #print (list_flat,list_index)
    dataframe = pd.DataFrame({x:list_flat},index=list_index)
    dfa["device%s"%count] = pd.concat([dfa["device%s"%count],dataframe],axis=1,sort=False)
    
    #print(dfa["device%s"%count])

    dfa["device%s"%count].columns=["VRFNames","LGT","RGT","SGACL","ACLCONT"]

    del dfa["device%s"%count]["VRFNames"]
    
    dfa["device%s"%count]['SGACL'] = dfa["device%s"%count]['SGACL'].astype(str)
    dfa["device%s"%count]['RGT'] = dfa["device%s"%count]['RGT'].astype(str)
    dfa["device%s"%count]['ACLCONT'] = dfa["device%s"%count]['ACLCONT'].astype(str)
    dfa["device%s"%count].drop_duplicates(keep="first",inplace=True)
    
    if unknwnval==1:
        del vrfnames[-1]
    
    dfa1["device%s"%count]=pd.DataFrame({"VRFNames":vrfnames})
        
    x="iplist"
    list_flat,list_index=flat(eval(x))
    dataframe = pd.DataFrame({x:list_flat},index=list_index)
    dfa1["device%s"%count] = pd.concat([dfa1["device%s"%count],dataframe],axis=1,sort=False)    
    
    x="sgtvrf"
    list_flat,list_index=flat(eval(x))
    #print (list_flat,list_index)
    dataframe = pd.DataFrame({x:list_flat},index=list_index)
    dfa1["device%s"%count] = pd.concat([dfa1["device%s"%count],dataframe],axis=1,sort=False)
    
    dfa1["device%s"%count].columns=["VRFNames","IP","LGT"]
    dfa1["device%s"%count].drop_duplicates(keep="first",inplace=True)
    
    pd.set_option('display.max_colwidth', -1)
    
    #print (dfa["device%s"%count])

#function to return differences between 2 lists
def returnNotMatches(li1, li2): 
    return (list(set(li1) - set(li2)))

#===================================Consistency check===========================================================
print("Consistency check")
output1={}
output2={}
#print(exist)
pd.set_option('display.max_colwidth', -1)
for p in exist:
    res=[]
    res1=[]
    d=[]
    for a,c in zip(sgtbig["device%s"%p],dgtbig["device%s"%p]):
        b=[]
        if a in d:
            continue
        if a:
            d.append(a)
            b.append(a)
            df_temp = df1.loc[df1['dgtval'].isin(b)]
            df_temp.columns=['DF','name','SGT','DGT','SGACL','ACL_ISE']
            del df_temp['DF']
            del df_temp['name']
            columnsTitles=["DGT","SGT","SGACL","ACL_ISE"]
            df_temp=df_temp.reindex(columns=columnsTitles)
            df_temp = df_temp.sort_values(by=['SGT'])
            df_temp=df_temp.reset_index(drop=True)
            if debug==1:
                print("ISE")
                print(df_temp)
            df_temp1 = dfa3["device%s"%p].loc[dfa3["device%s"%p]['LGT'].isin(b)]
            df_temp1.columns=['DGT','SGT','SGACL','ACL_device']
            df_temp1.sort_values(by=['SGT'])
            df_temp1=df_temp1.reset_index(drop=True)
            if debug==1:
                print("Network device")
                print(df_temp1)
            if not c:
                continue
            df_temp1['SGT']=df_temp1['SGT'].astype(int)
            df_temp1['DGT']=df_temp1['DGT'].astype(int)
            df_final1 = df_temp.merge(df_temp1, on=["DGT","SGT","SGACL"], how='outer')
            df_final1.columns=["1.DGT","2.SGT","3.SGACL","4.ACL_ISE","5.ACL_Device"]
            if debug==1:
                print(df_final1)
            di = df_final1.to_dict(orient='records')
            #print(di)
            table={}
            j=0
            for i in di:
                table.update({j:i})
                j=j+1
            #print(table)
            table = pd.DataFrame(table)
            #print(table)
            def isNaN(num):
                return num != num
            res.append(df_final1)
            res1.append(df_temp)
    output2.update({"device%s"%p:res1})
    output1.update({"device%s"%p:res})

out1={}    
success={}  

for p in exist:
    out=[]
    yes=[]
    if debug==1:
        print("device ",p)
    #print(dfa["device%s"%p]['LGT'])
    for a,b,c,d in zip(dfa["device%s"%p]['LGT'],dfa["device%s"%p]['RGT'],dfa["device%s"%p]['SGACL'],dfa["device%s"%p]['ACLCONT']):
        match=0
        if a=='':
            continue
        for e in df['dgtval']:
            #print(a,e)
            if(int(a)==e):  #comparing LGT with dgt
                #print(a,e)
                match=1
                if b:
                    b = ast.literal_eval(b)
                    for n in b:
                        if(isinstance(b, str)):
                            n.strip()
                if c:
                    c = ast.literal_eval(c)
                    for n in c:
                        #print(n)
                        if(isinstance(c, str)):
                            n.strip()
                if d:
                    d = ast.literal_eval(d)
                    for n in d:
                        #print(n)
                        if(isinstance(d, str)):
                            n.strip()
                f=[]
                g=[]
                h=[]
                for i,j,k,l in zip(df['dgtval'],df['sgtval'],df['sgaclval'],df['aclcont']):
                    if(i==e):                #comapring current dgt value with outer loop dgt value and if it matches
                        #print(i,e,type(i))
                        k = ast.literal_eval(k)
                        for n in k:
                            if(isinstance(k, str)):
                                n.strip()
                        l = ast.literal_eval(l)
                        for n in l:
                            #print(n)
                            if(isinstance(n, str)):
                                n.strip()
                        f.append(str(j))     #append all sgt, sgacl and aclcont that corresponds to dgt
                        g.append(k)
                        h.append(l)
                #print(f,b,g,c,h,d)        
                f, g, h = (list(t) for t in zip(*sorted(zip(f, g, h))))
                #print(type(f),type(b),type(g),type(c),type(h),type(d))
                f, g, h = (list(t) for t in zip(*sorted(zip(f, g, h))))
                b, c, d = (list(t) for t in zip(*sorted(zip(b, c, d))))
                if debug==1:
                    print("final", f,b,g,c,h,d)
                if(f==b):   #check if all sgt is there in device ie sgt==rgt
                    if(g==c and h==d): #check if all sgacl and aclcont is there in device ie sgacl==SGACL and acl==ACLCONT
                        #print("policy %s download okay"%a)
                        if a=='0':
                            out.append("policy 'unknown' download okay")
                            #print("policy 'unknown' download okay")
                        else:
                            out.append("policy %s download okay"%a)
                            #print("policy %s download okay"%a)
                        yes.append('')
                        break
                    else: #if sgacl didnt match SGACL print the missing acls in SGACL
                        match1=returnNotMatches(g,c)
                        if not match1:
                            match1=returnNotMatches(c,g)
                        if a=='0':
                            out.append("policy 'unknown' download not okay as these acl's are missing: {}".format(match1))
                            if debug==1:
                                print("policy 'unknown' download not okay as these acl's are missing: {}".format(match1))
                        else:    
                            out.append("policy {} download not okay as these acl's are missing: {}".format(a,match1))
                            if debug==1:
                                print("policy {} download not okay as these acl's are missing: {}".format(a,match1))
                    break
                else: #if sgt didnt match rgt print missing sgt in rgt
                    match1=returnNotMatches(f,b)
                    if not match1:
                        match1=returnNotMatches(b,f)
                    if a=='0':
                        out.append("policy 'unknown' download not okay as these remote group tags (source) are missing: {}".format(match1))
                        if debug==1:
                            print("policy 'unknown' download not okay as these remote group tags (source) are missing: {}".format(match1))
                    else:
                        out.append("policy {} download not okay as these remote group tags (source) are missing: {}".format(a,match1))
                        if debug==1:
                            print("policy {} download not okay as these remote group tags (source) are missing: {}".format(a,match1))
                break
        #print(match)
        if match == 0: #LGT wasnt found in dgt list at all
            if a=='0':
                out.append("policy 'unknown' download not okay as {} local group tag (dest) is missing".format(a))
                if debug==1:
                    print("policy 'unknown' download not okay as {} local group tag (dest) is missing".format(a))
            else:
                out.append("policy {} download not okay as {} local group tag (dest) is missing".format(a,a))
                if debug==1:
                    print("policy {} download not okay as {} local group tag (dest) is missing".format(a,a))
    out1.update({"device%s"%p:out})
    if(len(yes)==len(out)):
        success.update({"device%s"%p:"success"})
    else:
        success.update({"device%s"%p:"failed"})    

if debug==1:       
    print(df2)
    print(dfa2)
    print(dfa1)
    print(pacinfo)
    print(sxpinfo)
    print(DEVICES)
    print(success)
    print(out1)
    print(output1)


#================================to generate pdf===================================================
print("Generating PDF")
import numpy as np
from reportlab.lib.styles import ParagraphStyle as PS
from reportlab.platypus import PageBreak
from reportlab.platypus.paragraph import Paragraph
from reportlab.platypus.doctemplate import PageTemplate, BaseDocTemplate
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.platypus.frames import Frame
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm

class MyDocTemplate(BaseDocTemplate):
    def __init__(self, filename, **kw):
        self.allowSplitting = 0
        BaseDocTemplate.__init__(self, filename, **kw)
        template = PageTemplate('normal', [Frame(2.5*cm, 2.5*cm, 15*cm, 25*cm, id='F1')])
        self.addPageTemplates(template)
                
    def afterFlowable(self, flowable):
        "Registers TOC entries."
        if flowable.__class__.__name__ == 'Paragraph':
            text = flowable.getPlainText()
            style = flowable.style.name
            if style == 'Heading1':
                level = 0
            elif style == 'Heading2':
                level = 1
            else:
                return
            E = [level, text, self.page]
            
            #if we have a bookmark name append that to our notify data
            bn = getattr(flowable,'_bookmarkName',None)
            if bn is not None: E.append(bn)
            self.notify('TOCEntry', tuple(E))
            
centered = PS(name = 'centered',
    fontSize = 30,
    leading = 16,
    alignment = 1,
    spaceAfter = 20)

h1 = PS(name = 'Heading1',
              fontSize = 14,
             leading = 16,
               alignment=1,
           fontName='Helvetica-Bold')
h2 = PS(name = 'Heading2',
              fontSize = 14,
              leading = 16,
              alignment=1,
           fontName='Helvetica-Bold')

element=[]

element.append(Paragraph('<b>Table of contents</b>', centered))

toc = TableOfContents()
#toc.levelStyles = [h1, h2]
toc.levelStyles = [
    PS(fontName='Times-Bold', fontSize=20, name='TOCHeading1', leftIndent=20, firstLineIndent=-20, spaceBefore=10, leading=16),
    PS(fontSize=18, name='TOCHeading2', leftIndent=40, firstLineIndent=-20, spaceBefore=5, leading=12),
]

element.append(toc)
element.append(PageBreak())

def doHeading(text,sty):
    from hashlib import sha1
    #create bookmarkname
    bn=sha1((text+sty.name).encode('utf-8')).hexdigest()
    #modify paragraph text to include an anchor point with name bn
    h=Paragraph(text+'<a name="%s"/>' % bn,sty)
    #store the bookmark name on the flowable so afterFlowable can see this
    h._bookmarkName=bn
    element.append(h)
    
PS = ParagraphStyle

style = ParagraphStyle(
    name='Normal',
    fontName='Helvetica-Bold',
    fontSize=14,
    alignment=1
)
style1 = ParagraphStyle(
    name='Normal',
    fontName='Helvetica',
    fontSize=10,
    alignment=1
)

a= np.array(df2)
#print(a)
li = ['defaultRule', 'name', 'sgtval', 'dgtval', 'sgacl', 'aclcont']
b = np.array(li)
#print(b)
p = np.vstack((b, a))
#print(p)
t1 = Table(np.array(p).tolist());
t1.setStyle(TableStyle([('GRID',(0,0),(-1,-1),1,colors.black),
                        ('TEXTCOLOR',(0,0),(-1,0),colors.red),
                        ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold')]))
doHeading("ISE",h1)
element.append(Spacer(1, 0.2 * inch))
element.append(t1)
element.append(PageBreak())
for i in exist:
    j="device%s"%i
    #print(j)
    doHeading("home",h3)
    doHeading(DEVICES[j]['type'],h2)
    element.append(Spacer(1, 0.2 * inch))
    element.append(Paragraph("Management IP Address: %s"%DEVICES[j]['managementIpAddress'],style1))
    element.append(Spacer(1, 0.1 * inch))
    element.append(Paragraph(pacinfo[j],style1))
    element.append(Spacer(1, 0.1 * inch))
    element.append(Paragraph(sxpinfo[j],style1))
    element.append(Spacer(1, 0.2 * inch))
    element.append(Paragraph("ISE",style))
    element.append(Spacer(1, 0.2 * inch))
    li = ['DGT','SGT','SGACL_ISE','ACL_ISE']
    b = np.array(li)
    #print(b)
    for k in output2[j]:
        a= np.array(k)
       # print(a)
        p = np.vstack((b, a))
        #print(p)
        t2 = Table(np.array(p).tolist());
        #print(t2)
        t2.setStyle(TableStyle([('GRID',(0,0),(-1,-1),1,colors.black),
                        ('TEXTCOLOR',(0,0),(-1,0),colors.red),
                        ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold')]))
        element.append(t2)
        element.append(Spacer(1, 0.1 * inch))
    element.append(Spacer(1, 0.2 * inch))
    element.append(Paragraph("Network Device(from DNAC)",style))
    element.append(Spacer(1, 0.2 * inch))
    a=np.array(dfa1[j])
    #print(a)
    li = ['VRFNames','IP','LGT']
    b=np.array(li)
    #print(b)
    p = np.vstack((b,a))
    #print(p)
    t3 = Table(np.array(p).tolist());
    t3.setStyle(TableStyle([('GRID',(0,0),(-1,-1),1,colors.black),
                        ('TEXTCOLOR',(0,0),(-1,0),colors.red),
                        ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold')]))
    #print(t3)
    a=np.array(dfa2[j])
    li = ['LGT','RGT','SGACL','ACLCONT']
    b=np.array(li)
    p = np.vstack((b,a))
    t4 = Table(np.array(p).tolist());
    t4.setStyle(TableStyle([('GRID',(0,0),(-1,-1),1,colors.black),
                        ('TEXTCOLOR',(0,0),(-1,0),colors.red),
                        ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold')]))
    element.append(t3)
    element.append(Spacer(1, 0.1 * inch))
    element.append(t4)
    element.append(Spacer(1, 0.2 * inch))
    element.append(Paragraph("Consistency checker information",style))
    element.append(Spacer(1, 0.2 * inch))
    li = ['DGT','SGT','SGACL','ACL_ISE','ACL_Device']
    b = np.array(li)
    #print(b)
    for l in out1[j]:
        element.append(Paragraph(l,style1))
        element.append(Spacer(1, 0.1 * inch))
    element.append(Spacer(1, 0.2 * inch))
    for k in output1[j]:
        s=[]
        print(k)
        dfb = k[isNaN(k['5.ACL_Device'])].index.values.astype(int)
        print(dfb)
        dfb1=(k[isNaN(k['4.ACL_ISE'])].index.values.astype(int))
        print(dfb1)
        a= np.array(k)
       # print(a)
        p = np.vstack((b, a))
        #print(p)
        t5 = Table(np.array(p).tolist());
        t5.setStyle(TableStyle([('GRID',(0,0),(-1,-1),1,colors.black),
                        ('TEXTCOLOR',(0,0),(-1,0),colors.red),
                        ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold')]))
        if dfb.size>0:
            for i in dfb:
                print(i+1)
                t5.setStyle(TableStyle([('BACKGROUND',(4,i+1),(4,i+1),colors.red)]))
        if dfb1.size>0:
            for j in dfb1:
                print(j+1)
                t5.setStyle(TableStyle([('BACKGROUND',(3,j+1),(3,j+1),colors.red)]))
        #print(t5)
        for ix in k.index:
            if isNaN(k.loc[ix]['4.ACL_ISE']) or isNaN(k.loc[ix]['5.ACL_Device']):
                continue
            if not k.loc[ix]['4.ACL_ISE']==k.loc[ix]['5.ACL_Device']:
                s.append(ix)
        if s:
            for i in s:
                t5.setStyle(TableStyle([('BACKGROUND',(3,i+1),(4,i+1),colors.yellow)]))
        element.append(t5)
        element.append(Spacer(1, 0.1 * inch)) 
    element.append(Spacer(1, 0.2 * inch))
    element.append(PageBreak())

doc = MyDocTemplate('table.pdf')    

doc.multiBuild(element)
print("table.pdf is ready to view")