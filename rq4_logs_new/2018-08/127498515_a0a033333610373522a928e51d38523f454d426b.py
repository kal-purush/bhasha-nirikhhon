
#############picture###########    

from firebase import firebase
from google.cloud import storage
client = storage.Client.from_service_account_json('C:\Users\MOJI\Desktop\project\dogwood-terra-184417-firebase-adminsdk-vy9o9-765ac92f9f.json')
bucket = client.get_bucket('dogwood-terra-184417.appspot.com')
firebase = firebase.FirebaseApplication('https://dogwood-terra-184417.firebaseio.com/', None)
import requests
import os
def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)
import sqlite3
conn = sqlite3.connect('C:\Users\MOJI\Desktop\project\Firebase\python\Test_PJ2.db')
cursor = conn.execute("SELECT ID,Name,day,month,year,hour,minute from call_Detect")
result = firebase.get('/object/data/',None)
n = []
Na=[]
sameDay=[]
sameMonth=[]
sameYear=[]
sameHour=[]
sameMinute=[]
id2=[]
Maxid=0

for row in cursor:
    id2.append(row[0])
    Na.append(row[1])
    sameDay.append(row[2])
    sameMonth.append(row[3])
    sameYear.append(row[4])
    sameHour.append(row[5])
    sameMinute.append(row[6])
    if(row[0]>Maxid):
        Maxid=row[0]
cursor.close()
#print NaM
#print sameY
for i in result :
    name=firebase.get('/object/data/'+i,'object_Name')
    n.append(name)
    
#print n
#print Na 
Maxid = Maxid+1
nobcheck = set(n).difference(Na)
nobcheck = list(nobcheck)
ln=len(nobcheck)
#print nobcheck
nobcheck2 = set(Na).difference(n)
nobcheck2 = list(nobcheck2)
ln2=len(nobcheck2)
#print nobcheck2
ln6=len(Na)
count=0
count1=0
count2=0

for j in range(ln6):
    for i in result :
        resultchek = firebase.get('/object/data/'+i,None)
        listre=list(resultchek)
        lnre=len(listre)-2
        if(resultchek!=None):
            name=firebase.get('/object/data/'+i,'object_Name')
            date=firebase.get('/object/data/'+i,'date')
            listdate = date.split(",")
            #print(type(listdate[0]))
            if(count1<ln6):
                if(name==Na[count1]):
                    print "..................."
                    print name
                    print sameYear[count1]
                    print Na[count1]
                    s=0
                    f=0
                    yearF=int(listdate[2])
                    monthF=int(listdate[1])
                    dayF=int(listdate[0])
                    hourF=int(listdate[3])
                    minuteF=int(listdate[4])
                    if(yearF == sameYear[count1]): #ปีเท่ากัน
                        if(monthF == sameMonth[count1]):#เดือนเท่ากัน
                            if(dayF == sameDay[count1]):#วันเท่ากัน
                                if(hourF == sameHour[count1]):#ชั่วโมงเท่ากัน
                                    if(minuteF==sameMinute[count1]):
                                        print 'equal'
                                    elif(minuteF > sameMinute[count1]):
                                        print "dayF"
                                        f = 1
                                    else :
                                        s = 1
                                        print "minuteS"  
                                elif(hourF > sameHour[count1]):
                                    print "hourF"
                                    f = 1
                                else:
                                    s = 1
                                    print "hourS"
                            elif(dayF > sameDay[count1]):
                                print "dayF"
                                f = 1
                            else:
                                s = 1
                                print "dayS"
                        elif(monthF > sameMonth[count1]):
                            print "monthF"
                            f = 1
                        else: 
                            s = 1
                            #print (monthS)
                    elif(yearF > sameYear[count1]):
                        print "yearF"
                        f = 1
                    else: 
                        s = 1
                        print "yearS"
                    if(s==1):
                        print 's'
                        print sameDay[count1],sameMonth[count1],sameYear[count1],sameHour[count1],sameMinute[count1]
                        fileaddress = ("C:\Users\MOJI\Pictures\\")+ str(row[1])
                        print fileaddress
                        filename1 = str(row[1])+'1_1.jpg'
                        blob1 = bucket.blob(str(row[1])+'1_1.jpg')
                        blob1.upload_from_filename(filename = fileaddress +"\\"+ filename1)
                        filename2 = str(row[1])+'1_2.jpg'
                        blob2 = bucket.blob(str(row[1])+'1_2.jpg')
                        blob2.upload_from_filename(filename = fileaddress +"\\"+ filename2)
                        filename3 = str(row[1])+'1_3.jpg'
                        blob3 = bucket.blob(str(row[1])+'1_3.jpg')
                        blob3.upload_from_filename(filename = fileaddress +"\\"+ filename3)
                        filename4 = str(row[1])+'1_4.jpg'
                        blob4 = bucket.blob(str(row[1])+'1_4.jpg')
                        blob4.upload_from_filename(filename = fileaddress +"\\"+ filename4)
                        filename5 = str(row[1])+'1_5.jpg'
                        blob5 = bucket.blob(str(row[1])+'1_5.jpg')
                        blob5.upload_from_filename(filename = fileaddress +"\\"+ filename5)
                        filename6 = str(row[1])+'1_6.jpg'
                        blob6 = bucket.blob(str(row[1])+'1_6.jpg')
                        blob6.upload_from_filename(filename = fileaddress +"\\"+ filename6)
                        filename7 = str(row[1])+'1_7.jpg'
                        blob7 = bucket.blob(str(row[1])+'1_7.jpg')
                        blob7.upload_from_filename(filename = fileaddress +"\\"+ filename7)
                        filename8 = str(row[1])+'1_8.jpg'
                        blob8 = bucket.blob(str(row[1])+'1_8.jpg')
                        blob8.upload_from_filename(filename = fileaddress +"\\"+ filename8)
                        filename9 = str(row[1])+'1_9.jpg'
                        blob9 = bucket.blob(str(row[1])+'1_9.jpg')
                        blob9.upload_from_filename(filename = fileaddress +"\\"+ filename9)
                        filename10 = str(row[1])+'1_10.jpg'
                        blob10 = bucket.blob(str(row[1])+'1_10.jpg')
                        blob10.upload_from_filename(filename = fileaddress +"\\"+ filename10)
                        result4 = firebase.delete('/object/data/'+i,None)
                        firebase.post('/object/data',{'object_Name':str(row[1]),'thumbnail':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename1+'?alt=media'
                                                      ,'thumbnail2':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename2+'?alt=media'
                                                      ,'thumbnail3':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename3+'?alt=media'
                                                      ,'thumbnail4':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename4+'?alt=media'
                                                      ,'thumbnail5':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename5+'?alt=media'
                                                      ,'thumbnail6':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename6+'?alt=media'
                                                      ,'thumbnail7':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename7+'?alt=media'
                                                      ,'thumbnail8':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename8+'?alt=media'
                                                      ,'thumbnail9':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename9+'?alt=media'
                                                      ,'thumbnail10':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename10+'?alt=media'
                                                      ,'date':str(sameDay[count1])+','+str(sameMonth[count1])+','+str(sameYear[count1])+','+str(sameHour[count1])+','+str(sameMinute[count1])})
                    elif(f==1):
                        print 'f'
                        conn = sqlite3.connect('C:\Users\MOJI\Desktop\project\Firebase\python\Test_PJ2.db')
                        conn.execute("DELETE from call_Detect where ID = "+str(id2[count1])+";")
                        conn.commit()
                        with sqlite3.connect('C:\Users\MOJI\Desktop\project\Firebase\python\Test_PJ2.db') as con:
                                cur3 = con.cursor()
                                cur3.execute('insert into call_Detect values (?,?,?,?,?,?,?,?)',(id2[count1],name,1,int(listdate[0]),int(listdate[1]),int(listdate[2]),int(listdate[3]),int(listdate[4])))
                        for k in range(lnre):
                            print k
                            if(k==0):
                                url = firebase.get('/object/data/'+i,'thumbnail')
                                print url,'111'
                            else:
                                url = firebase.get('/object/data/'+i,'thumbnail'+str(k+1))
                                print url
                            r = requests.get(url)   
                            folder = createFolder('C:\Users\MOJI\Pictures./'+name+'/')
                            with open('C:\\Users\\MOJI\\Pictures\\'+name+'\\'+name+str(k+1)+'.jpg', 'wb') as f:f.write(r.content)
    count1+=1

result = firebase.get('/object/data/',None)
for j in range(ln):
    for i in result :
        resultchek = firebase.get('/object/data/'+i,None)
        listre=list(resultchek)
        lnre=len(listre)-2
        if(resultchek!=None):
            name=firebase.get('/object/data/'+i,'object_Name')
            date=firebase.get('/object/data/'+i,'date')
            listdate = date.split(",")
            if(count<ln):
                if(name==nobcheck[count]):
                    print name,Maxid,int(listdate[0]),int(listdate[1]),int(listdate[2]),int(listdate[3]),int(listdate[4])   
                    with sqlite3.connect('C:\Users\MOJI\Desktop\project\Firebase\python\Test_PJ2.db') as con:
                        cur3 = con.cursor()
                        cur3.execute('insert into call_Detect values (?,?,?,?,?,?,?,?)',(Maxid,name,1,int(listdate[0]),int(listdate[1]),int(listdate[2]),int(listdate[3]),int(listdate[4])))
                    #print lnre
                    for k in range(lnre):
                        print k
                        if(k==0):
                            url = firebase.get('/object/data/'+i,'thumbnail')
                            print url,'111'
                        else:
                            url = firebase.get('/object/data/'+i,'thumbnail'+str(k+1))
                            print url
                        r = requests.get(url)   
                        folder = createFolder('C:\Users\MOJI\Downloads./'+name+'/')
                        with open('C:\\Users\\MOJI\\Downloads\\'+name+'\\'+name+str(k+1)+'_1'+'.jpg', 'wb') as f:f.write(r.content)
    count+=1
    Maxid+=1

cursor = conn.execute("SELECT ID, Name,K,day,month,year,hour,minute from call_Detect")
for j in range(ln2):
    for row in cursor:
        if(count2<ln2):
            if(row[1]==nobcheck2[count2]):
                print row[1]
                day=row[3]
                month=row[4]
                year=row[5]
                hour=row[6]
                minute=row[7]
                print day,month,year,hour,minute
                fileaddress = ("C:\Users\MOJI\Pictures\\")+ str(row[1])
                print fileaddress
                filename1 = str(row[1])+'1.jpg'
                blob1 = bucket.blob(str(row[1])+'1.jpg')
                blob1.upload_from_filename(filename = fileaddress +"\\"+ filename1)
                filename2 = str(row[1])+'2.jpg'
                blob2 = bucket.blob(str(row[1])+'2.jpg')
                blob2.upload_from_filename(filename = fileaddress +"\\"+ filename2)
                filename3 = str(row[1])+'3.jpg'
                blob3 = bucket.blob(str(row[1])+'3.jpg')
                blob3.upload_from_filename(filename = fileaddress +"\\"+ filename3)
                filename4 = str(row[1])+'4.jpg'
                blob4 = bucket.blob(str(row[1])+'4.jpg')
                blob4.upload_from_filename(filename = fileaddress +"\\"+ filename4)
                filename5 = str(row[1])+'5.jpg'
                blob5 = bucket.blob(str(row[1])+'5.jpg')
                blob5.upload_from_filename(filename = fileaddress +"\\"+ filename5)
                filename6 = str(row[1])+'6.jpg'
                blob6 = bucket.blob(str(row[1])+'6.jpg')
                blob6.upload_from_filename(filename = fileaddress +"\\"+ filename6)
                filename7 = str(row[1])+'7.jpg'
                blob7 = bucket.blob(str(row[1])+'7.jpg')
                blob7.upload_from_filename(filename = fileaddress +"\\"+ filename7)
                filename8 = str(row[1])+'8.jpg'
                blob8 = bucket.blob(str(row[1])+'8.jpg')
                blob8.upload_from_filename(filename = fileaddress +"\\"+ filename8)
                filename9 = str(row[1])+'9.jpg'
                blob9 = bucket.blob(str(row[1])+'9.jpg')
                blob9.upload_from_filename(filename = fileaddress +"\\"+ filename9)
                filename10 = str(row[1])+'10.jpg'
                blob10 = bucket.blob(str(row[1])+'10.jpg')
                blob10.upload_from_filename(filename = fileaddress +"\\"+ filename10)
                firebase.post('/object/data',{'object_Name':str(row[1]),'thumbnail':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename1+'?alt=media'
                                              ,'thumbnail2':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename2+'?alt=media'
                                              ,'thumbnail3':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename3+'?alt=media'
                                              ,'thumbnail4':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename4+'?alt=media'
                                              ,'thumbnail5':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename5+'?alt=media'
                                              ,'thumbnail6':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename6+'?alt=media'
                                              ,'thumbnail7':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename7+'?alt=media'
                                              ,'thumbnail8':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename8+'?alt=media'
                                              ,'thumbnail9':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename9+'?alt=media'
                                              ,'thumbnail10':'https://firebasestorage.googleapis.com/v0/b/dogwood-terra-184417.appspot.com/o/'+filename10+'?alt=media'
                                              ,'date':str(day)+','+str(month)+','+str(year)+','+str(hour)+','+str(minute)})
    count2+=1
cursor.close()

###################### motor #################
            
from firebase import firebase
from google.cloud import storage
client = storage.Client.from_service_account_json('C:\Users\MOJI\Desktop\project\dogwood-terra-184417-firebase-adminsdk-vy9o9-765ac92f9f.json')
bucket = client.get_bucket('dogwood-terra-184417.appspot.com')
firebase = firebase.FirebaseApplication('https://dogwood-terra-184417.firebaseio.com/', None)
import sqlite3
conn = sqlite3.connect('C:\Users\MOJI\Desktop\project\Firebase\python\Test_PJ2.db')
cursor = conn.execute("SELECT ID,Name,day,month,year,hour,minute from ActionName")
result = firebase.get('/action/data/',None)
nM = []
NaM=[]
sameD=[]
sameM=[]
sameY=[]
sameH=[]
sameMi=[]
id1=[]
maxid=0
for row in cursor:
    id1.append(row[0])
    NaM.append(row[1])
    sameD.append(row[2])
    sameM.append(row[3])
    sameY.append(row[4])
    sameH.append(row[5])
    sameMi.append(row[6])
    if(row[0]>maxid):
        maxid=row[0]
cursor.close()

#print NaM
#print sameY
for i in result :
    name=firebase.get('/action/data/'+i,'action_name')
    nM.append(name)
#print sameNM,sameDt
#print lnsameNM
#print n
#print Na 
#print maxid
maxid = maxid+1
nMocheck = set(nM).difference(NaM)
nMocheck = list(nMocheck)
ln3=len(nMocheck)
#print ln3
#print nMocheck
nMocheck2 = set(NaM).difference(nM)
nMocheck2 = list(nMocheck2)
ln4=len(nMocheck2)
#print ln4,nMocheck2
ln5=len(NaM)
count3=0
count4=0
count5=0

for j in range(ln5):
    for i in result :
        resultchek = firebase.get('/action/data/'+i,None)
        if(resultchek!=None):
            name=firebase.get('/action/data/'+i,'action_name')
            motor=firebase.get('/action/data/'+i,'motor')
            motor = str(motor)
            motor = motor.split() #ตัดช่องว่าง
            listmotor = motor[1].split(",")
            date=firebase.get('/action/data/'+i,'date')
            listdate = date.split(",")
            #print(type(listdate[0]))
            step =firebase.get('/action/data/'+i,'step')
            if(count5<ln5):
                if(name==NaM[count5]):
                    print "..................."
                    print name
                    print sameY[count5]
                    print NaM[count5]
                    s=0
                    f=0
                    yearF=int(listdate[2])
                    monthF=int(listdate[1])
                    dayF=int(listdate[0])
                    hourF=int(listdate[3])
                    minuteF=int(listdate[4])
                    if(yearF == sameY[count5]): #ปีเท่ากัน
                        if(monthF == sameM[count5]):#เดือนเท่ากัน
                            if(dayF == sameD[count5]):#วันเท่ากัน
                                if(hourF == sameH[count5]):#ชั่วโมงเท่ากัน
                                    if(minuteF==sameMi[count5]):
                                        print 'equal'
                                    elif(minuteF > sameMi[count5]):
                                        print "dayF"
                                        f = step
                                    else :
                                        s = step
                                        print "minuteS"  
                                elif(hourF > sameH[count5]):
                                    print "hourF"
                                    f = step
                                else:
                                    s = step
                                    print "hourS"
                            elif(dayF > sameD[count5]):
                                print "dayF"
                                f = step
                            else:
                                s = step
                                print "dayS"
                        elif(monthF > sameM[count5]):
                            print "monthF"
                            f = step
                        else: 
                            s = step
                            #print (monthS)
                    elif(yearF > sameY[count5]):
                        print "yearF"
                        f = step
                    else: 
                        s = step
                        print "yearS"
                    if(step==s):
                        for j in result :
                            result3 = firebase.get('/action/data/'+j,'action_name')
                            if(str(result3)==str(NaM[count5])):
                                result4 = firebase.delete('/action/data/'+j,None)
                        cursor3 = conn.execute("SELECT ID, stepAction,M1,M2,M3,M4,M5,M6,M7,M8 from Action_Robot")
                        for row3 in cursor3:
                            if(id1[count5]==row3[0]):
                                firebase.post('/action/data', {'action_name': NaM[count5],'step':row3[1],'motor': "[ "+str(row3[2])+","+str(row3[3])+","+str(row3[4])+","+str(row3[5])+","+str(row3[6])+","+str(row3[7])+","+str(row3[8])+","+str(row3[9])+" ]",'date':str(row[2])+","+str(row[3])+","+str(row[4])+","+str(row[5])+","+str(row[6])})
                        cursor3.close()     
                    elif(step==f):
                        print name,"aaaa",step,'bbb',id1[count5]
                        conn = sqlite3.connect('C:\Users\MOJI\Desktop\project\Firebase\python\Test_PJ2.db')
                        conn.execute("DELETE from ActionName where ID = "+str(id1[count5])+";")
                        conn.commit()
                        conn = sqlite3.connect('C:\Users\MOJI\Desktop\project\Firebase\python\Test_PJ2.db')
                        conn.execute("DELETE from Action_Robot where ID = "+str(id1[count5])+";")
                        conn.commit()
                        if(str(step)=='1'):
                            with sqlite3.connect('C:\Users\MOJI\Desktop\project\Firebase\python\Test_PJ2.db') as con:
                                cur3 = con.cursor()
                                cur3.execute('insert into ActionName values (?,?,?,?,?,?,?)',(int(id1[count5]),str(name),int(listdate[0]),int(listdate[1]),int(listdate[2]),int(listdate[3]),int(listdate[4])))
                    
                        with sqlite3.connect('C:\Users\MOJI\Desktop\project\Firebase\python\Test_PJ2.db') as con:
                                cur4 = con.cursor()
                                cur4.execute('insert into Action_Robot values (?,?,?,?,?,?,?,?,?,?)',(int(id1[count5]),int(step),int(listmotor[0]),int(listmotor[1]),int(listmotor[2]),int(listmotor[3]),int(listmotor[4]),int(listmotor[5]),int(listmotor[6]),int(listmotor[7])))
    count5+=1

result = firebase.get('/action/data/',None)
for j in range(ln3):
    for i in result :
        resultchek = firebase.get('/action/data/'+i,None)
        if(resultchek!=None):
            name=firebase.get('/action/data/'+i,'action_name')
            motor=firebase.get('/action/data/'+i,'motor')
            motor = str(motor)
            motor = motor.split() #ตัดช่องว่าง
            listmotor = motor[1].split(",")
            date=firebase.get('/action/data/'+i,'date')
            listdate = date.split(",")
            #print(type(listdate[0]))
            step =firebase.get('/action/data/'+i,'step')
            #print name
            if(count3<ln3):
                if(name==nMocheck[count3]):  
                    print name,listdate,count3,maxid
                    if(str(step)=='1'):
                        with sqlite3.connect('C:\Users\MOJI\Desktop\project\Firebase\python\Test_PJ2.db') as con:
                                cur3 = con.cursor()
                                cur3.execute('insert into ActionName values (?,?,?,?,?,?,?)',(maxid,str(name),int(listdate[0]),int(listdate[1]),int(listdate[2]),int(listdate[3]),int(listdate[4])))
                    
                    with sqlite3.connect('C:\Users\MOJI\Desktop\project\Firebase\python\Test_PJ2.db') as con:
                                cur4 = con.cursor()
                                cur4.execute('insert into Action_Robot values (?,?,?,?,?,?,?,?,?,?)',(maxid,int(step),int(listmotor[0]),int(listmotor[1]),int(listmotor[2]),int(listmotor[3]),int(listmotor[4]),int(listmotor[5]),int(listmotor[6]),int(listmotor[7])))    
    count3+=1
    maxid+1
  
cursor = conn.execute("SELECT ID, Name,Day,Month,Year,Hour,Minute from ActionName")   
for j in range(ln4):
    for row in cursor:
        if(count4<ln4):
            if(row[1]==nMocheck2[count4]):
                print row[1]
                day=row[2]
                month=row[3]
                year=row[4]
                hour=row[5]
                minute=row[6]
                print day,month,year,hour,minute
                cursor2 = conn.execute("SELECT ID, stepAction,M1,M2,M3,M4,M5,M6,M7,M8 from Action_Robot")
                for row2 in cursor2:
                    if(row[0]==row2[0]):
                        firebase.post('/action/data', {'action_name': row[1],'step':row2[1],'motor': "[ "+str(row2[2])+","+str(row2[3])+","+str(row2[4])+","+str(row2[5])+","+str(row2[6])+","+str(row2[7])+","+str(row2[8])+","+str(row2[9])+" ]",'date':str(row[2])+","+str(row[3])+","+str(row[4])+","+str(row[5])+","+str(row[6])})
                cursor2.close()
    count4+=1
cursor.close()
result2 = firebase.get('/updatecheck/data/',None)
if result2!=None :
    result2 = firebase.delete('/updatecheck/data/',None)  
firebase.post('/updatecheck/data', {'check':"information match"})
firebase.post('/robotsend/data', {'textrobot':"Update completed."})