
# coding: utf-8

# In[ ]:

#TEST AGENT MODULE
from agent import *
a = []
a.append(Agent(1,10,10))
a.append(Agent(2,20,20))
a.append(Agent(3,30,30))

for i in range(len(a)):
    a[i].setUID(6)
    a[i].setIdeo(111)
    a[i].setWealth(123)
    a[i].setPower(456)
    a[i].setStakeholder(5)
    a[i].setColor("BRACK")
    a[i].setXCor(99)
    a[i].setYCor(99)
    a[i].setHidden(True)

    print(a[i].getUID())
    print(a[i].getIdeo())
    print(a[i].getWealth())
    print(a[i].getPower())
    print(a[i].getStakeholder())
    print(a[i].getColor())
    print(a[i].getXCor())
    print(a[i].getYCor())
    print(a[i].getHidden())


# In[ ]:

#TEST CITS MODULE
from cits import *

def SetObject(a,i):
    a.setUID(i)
    a.setIdeo(i)
    a.setWealth(i)
    a.setPower(i)
    a.setStakeholder(i)
    a.setColor(str(i))
    a.setXCor(i)
    a.setYCor(i)
    a.setHidden(True)

    a.setProximity(i)
    a.setParty(i)
    a.setSelectorate(i)
    a.setBought(i)
    a.setTemp_Eu(i)
    a.setSatisfaction (i)
    a.setEdu(i)
    a.setEdu_Scale (i)
    a.setRawpower(i)
    a.setMinpref(i)
    a.setTurcbo(i)
    a.setStemp_Eu(i)
    a.setSturcbo(i)
    for x in [Entity.PRF,Entity.POW,Entity.EU]:
        a.setOwn(x,i)
        a.setSown(x,i)
        a.setCbo(x,i)
        a.setScbo(x,i)
    return a

def PrintObj(a):
    print( a.getUID() )
    print( a.getIdeo() )
    print( a.getWealth() )
    print( a.getPower() )
    print( a.getStakeholder() )
    print( a.getColor() )
    print( a.getXCor() )
    print( a.getYCor() )
    print( a.getHidden() )
    print( a.getProximity() )
    print( a.getParty() )
    print( a.getSelectorate() )
    print( a.getBought() )
    print( a.getTemp_Eu() )
    print( a.getSatisfaction() )
    print( a.getEdu() )
    print( a.getEdu_Scale() )
    print( a.getRawpower() )
    print( a.getMinpref() )
    print( a.getTurcbo() )
    print( a.getStemp_Eu() )
    print( a.getSturcbo() )
    for x in [Entity.PRF,Entity.POW,Entity.EU]:
        print( x )
        print( "\t", a.getOwn(x))
        print( "\t", a.getSown(x))
        print( "\t", a.getCbo(x))
        print( "\t", a.getScbo(x))
        
c = CITS(22,33,44)
ac = SetObject(c,12)
PrintObj(ac)
bc = CITS(66,77,88)
PrintObj(ac)
PrintObj(bc)


# In[1]:

from citscollection import *


# In[3]:

mycits = CITS_Collection()
mycits.Initialize(10,10,10)
mycits.InitSatisfaction()
mycits.UpdatePower(123)
mycits.UpdateSatisfaction(100,0.1,22)
orig = mycits.getCITS( int(mycits.getNumCITS() / 2) )
mycits.NodesWithinRange(orig)


# In[ ]:


