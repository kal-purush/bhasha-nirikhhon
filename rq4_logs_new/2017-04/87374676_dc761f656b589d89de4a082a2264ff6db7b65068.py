
# coding: utf-8

# In[3]:

import random
from cits import *
from govts import *
from link_cits import *
from link_stakeholders import *

global_tax = 0
global_base = 0
global_talkspan = 10
global_govt_base_wealth = 0
global_gov_ideo = 90

MAX_XCOR = 100
MAX_YCOR = 100
MAX_TICKS = 24
CONFLICT_FLAG = False


class ComplexIPBModel:
    def __init__(self):
        self.price = 0
        self.supply = 0
        self.maxprox = 0
        self.maxpower = 0
        self.centerX = int(MAX_XCOR/2)
        self.centerY = int(MAX_YCOR/2)
        self.CITSarr = CITS_Collection()
        self.govts = Agent()
        self.linkcits = CITSLinkage(MAX_TICKS)
        self.dlinkstkhldrs = StakeholderLinkage()
        self.ticks = 0
    
    def Setup(self,initnum):
        #clear-all
        self.__init__()
    
        #create-govts 1 [...]
        self.govts.setStakeholder(True)
        self.govts.setHidden(True)

        #create-cits initial-number
        self.CITSarr.Initialize(initnum,max_x,max_y)
        
        #ask cits [ set satisfaction...]
        self.CITSarr.InitSatisfaction()
    
        #ask govts [ set wealth...]
        self.govts.setWealth((self.CITSarr.GetSum("wealth") * global_tax) + global_govt_base_wealth)
    
        self.ticks()

    #GO...
    def Step(self):
        if self.ticks >= MAX_TICKS or CONFLICT_FLAG:
            return -1
        
        #think this needs to only be called once... moved up from 
        self.linkcits.FormLinks(self,self.ticks,self.CITSarr)
                         
        self.Update()
        self.CITS_Talk()
        self.StakeholderTalk()
        self.Conflict()
        self.UpdatePlot()
        
        self.ticks += 1
    
    def Update(self):
        #set maxprox max [proximity] of cits
        self.maxprox = self.CITSarr.getMax("proximity")
        
        self.CITSarr.UpdateSatisfaction(global_base,global_tax,global_gov_ideo)
        
        #set maxpower max [rawpower] of cits
        self.maxpower = self.CITSarr.getMax("rawpower")

        #ask cits [...
        self.CITSarr.UpdatePower(self.maxpower)
        
        #ask govts [...
        #  set wealth ((sum ([wealth] of cits) * tax) + Government-Base-Wealth)
        govts.setWealth( (self.CITSarr.GetSum("wealth") * global_tax) + global_govt_base_wealth )
        govts.setPower( govts.getWealth() )
    
    
    def CITS_Talk(self):
        
        #Step through each node in the CITS array
        #create-linkcits-to cits in-radius talkspan with [who != [who] of myself]
        ## ^^^^ MOVED UP FOR EFFICIENCY ^^^^ ##
        
        #think this needs to only be called once...
        self.linkcits.UpdateLinks(self.ticks,self.CITSarr)
        
        self.linkcits.ManageLinks(self.ticks,self.CITSarr)

        
            
            if ((mcits[orig].getTemp_Eu() > mcits[orig].getOwn_Eu()) 
                and mcits[dest].getTemp_Eu() > (mcits[dest].getOwn_Eu())):
                
                mcits[orig].setTurcbo(2)
                mcits[orig].setStakeholder(True)
                
                mcits[orig].setOwn(Entity.E_PRF,getCurrentMaxOutlinks(orig,self.dlinkcits[self.ticks],Entity.E_PRF))
                mcits[orig].setSown(Entity.E_PRF,getCurrentMaxOutlinks(orig,self.dlinkcits[self.ticks],Entity.E_PRF))
                mcits[orig].setCbo(Entity.E_PRF,getCurrentMaxOutlinks(orig,self.dlinkcits[self.ticks],Entity.E_PRF))
                
                mcits[orig].setOwn(Entity.E_POW, (1.5 * mcits[orig].getOwn(Entity.E_POW)))
                mcits[orig].setSown(Entity.E_POW,getCurrentMaxOutlinks(orig,self.dlinkcits[self.ticks],Entity.E_POW))
                mcits[orig].setCbo(Entity.E_POW,getCurrentMaxOutlinks(orig,self.dlinkcits[self.ticks],Entity.E_POW))
                
                mcits[orig].setOwn(Entity.E_EU,getCurrentMaxOutlinks(orig,self.dlinkcits[self.ticks],Entity.E_EU))
                mcits[orig].setSown(Entity.E_EU,getCurrentMaxOutlinks(orig,self.dlinkcits[self.ticks],Entity.E_EU))
                mcits[orig].setCbo(Entity.E_EU,getCurrentMaxOutlinks(orig,self.dlinkcits[self.ticks],Entity.E_EU))
                
                mcits[dest].setTurcbo(2)
                mcits[dest].setStakeholder(False)
                
                mcits[dest].setOwn(Entity.E_PRF, mcits[orig].getCbo(Entity.E_PRF))
                mcits[dest].setCbo(Entity.E_PRF, mcits[orig].getCbo(Entity.E_PRF))
                
                mcits[dest].setOwn(Entity.E_POW, (1.5 * mcits[dest].getOwn(Entity.E_POW)) )
                mcits[dest].setCbo(Entity.E_POW, 0)
                
                mcits[dest].setOwn(Entity.E_EU, getCurrentMaxInlinks(dest,self.dlinkcits[self.ticks],Entity.E_EU))
                mcits[dest].setCbo(Entity.E_EU, getCurrentMaxInlinks(dest,self.dlinkcits[self.ticks],Entity.E_EU))
                
                #set hidden? FALSE #NEEDED????
            else:    
        
                mcits[link.getOrignode()].setTurcbo(1)###JUST RESET IT BACK???
                mcits[link.getOrignode()].setOwnpref own_pref ###WHO?
                mcits[link.getOrignode()].setOwn(Entity.E_POW, own_power)
            
                mcits[link.getDestnode()].setTurcbo(1)
                mcits[link.getDestnode()].setOwn(Entity.E_PRF, own_pref)
                smcits[link.getDestnode()].setOwn(Entity.E_POW, own_power)
            
                self.dlinkcits[self.ticks].remove(link)
            
        #ask linkcits with [citlink? < ticks]
        for link_idx in self.dlinkcits.keys():
            if link_idx < self.ticks:
                #if [own-pref] of end1 != [own-pref] of end2 [
                if mcits[orig].getCbo(Entity.E_PRF)!= mcits[dest].getCbo(Entity.E_PRF):
                    mcits[orig].setOwn(Entity.E_PRF, mcits[orig].getCbo(Entity.E_PRF))
                    mcits[orig].setOwn(Entity.E_POW, mcits[orig].getCbo(Entity.E_POW)
                    eu1 = mcits[orig].getCbo(Entity.E_EU)
                    
                    pref2 = mcits[dest].getCbo(Entity.E_PRF)
                    power2  = mcits[dest].getCbo(Entity.E_POW)
                    eu2 = mcits[dest].getCbo(Entity.E_EU)
                    
                    intereu = (power1 + power2) * 1.5 * (100 - abs(pref1 - pref2))
            
                    cboeu1 = 0.5 * (1.5 * eu1 + intereu)
                    cboeu2 = 0.5 * (1.5 * eu2 + intereu)
                    cbopref = ((pref1 * power1 + pref2 * power2)/(power1 + power2 + 0.0000001))
                    cbopower = (power1 + power2) * 1.5
                    cboeu = cbopower * (100 - abs (cbopref - cbopref))
                    cboeu12 = 0.5 * (eu1 + intereu) + 0.5 * (eu2 + intereu)
                    
                if (cboeu1 < mcits[orig].getCbo(Entity.E_EU)) or (cboeu2 < mcits[dest].getCbo(Entity.E_EU)):
                    #ask end1 [ // same as code below
                    if count my-out-links with [citlink? = 2] = 0 and count my-in-links with [citlink? = 2] = 0 [
                        mcits[orig].setTurcbo(1)
                        mcits[orig].setCbo(Entity.E_PRF,0)
                        mcits[orig].setCbo(Entity.E_POW,0)
                        mcits[orig].setStakeholder(False)
                        mcits[orig].setOwn(Entity.E_POW, (mcits[orig].getOwn(Entity.E_POW) / 1.5))
                        #mcits[orig].setOwn(Entity.E_EU, (100 - abs (own-pref - own-pref)) * mcits[orig].getOwn(Entity.E_POW))
                        mcits[orig].setOwn(Entity.E_EU, (100 * mcits[orig].getOwn(Entity.E_POW))
                        mcits[orig].setShape("circle")
                    
                    #ask end2 [// same as code above
                    if count my-out-links with [citlink? = 2] = 0 and count my-in-links with [citlink? = 2] = 0 [
                        mcits[dest].setTurcbo(1)
                        mcits[dest].setCbo(Entity.E_PRF,0)
                        mcits[dest].setCbo(Entity.E_POW,0)
                        mcits[dest].setStakeholder(False)
                        mcits[dest].setOwn(Entity.E_POW, (mcits[orig].getOwn(Entity.E_POW) / 1.5))
                        #mcits[dest].setOwn(Entity.E_EU, (100 - abs (own-pref - own-pref)) * mcits[dest].getOwn(Entity.E_POW))
                        mcits[dest].setOwn(Entity.E_EU, (100 * mcits[dest].getOwn(Entity.E_POW)))
                        mcits[dest].setShape("circle")
                    
                    self.dlinkcits[self.ticks].remove(link)
                                           
                else:
                    #ask end1 [
                    if count my-out-links with [citlink? = 2] = 0 and count my-in-links with [citlink? = 2] = 0 [
                        mcits[orig].setTurcbo(2)
                        mcits[orig].setStakeholder(False)
                        mcits[orig].setOwn(Entity.E_PRF, mcits[dest].getCbo(Entity.E_PRF))
                        mcits[orig].setCbo(Entity.E_PRF, mcits[dest].getCbo(Entity.E_PRF)
                        mcits[orig].setCbo(Entity.E_POW, 0)
                        mcits[orig].setOwn(Entity.E_EU, (100 * mcits[orig].getOwn(Entity.E_POW)))
                    else:
                        mcits[orig].setStakeholder(1)
                
                    #ask end2 [
                    if count my-out-links with [citlink? = 2] = 0 and count my-in-links with [citlink? = 2] = 0 [
                        mcits[dest].setTurcbo(2)
                        mcits[dest].setStakeholder(False)
                        mcits[dest].setOwn(Entity.E_PRF, mcits[orig].getCbo(Entity.E_PRF))
                        mcits[dest].setCbo(Entity.E_PRF, mcits[orig].getCbo(Entity.E_PRF)
                        mcits[dest].setCbo(Entity.E_POW, 0)
                        mcits[dest].setOwn(Entity.E_EU, (100 * mcits[dest].getOwn(Entity.E_POW)))
                    else:
                        mcits[dest].setStakeholder(1)
                
                    link.setHidden(False)
        
    
    ask cits with [stakeholder? = 1] [
        set cbo-power (sum [cbopower] of my-out-links with [citlink? > 0]) + (sum [cbopower] of my-in-links with [citlink? > 0]) - own-power * ((count my-out-links with [citlink? > 0]) + (count my-in-links with [citlink? > 0]) - 1)
    ]
    pass
    
    def StakeholderTalk(self):
        pass
    
    def Conflict(self):
        ask cits with [sturcbo? = 1] [
            if scbo-power >= sum [own-power] of cits with [sturcbo? != 1] * power-parity and 
                abs (scbo-pref - global_gov_ideo) > threshold:
            [
                set shape "exclamation"
                set size 5
                set color red
            ]
        ]
            


# In[ ]:


