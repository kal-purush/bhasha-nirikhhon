class Agent:
    def __init__(self,uid,x,y):
        self.UID = uid
        self.color = "#FFFFFF" #White
        self.shape = 'o'       #Circle
        self.xcor = x
        self.ycor = y
        self.hidden = False
        self.stakeholder = False
        self.ideo =0.0    #random.gauss(cit_ideo_mean, cit_ideo_sd)
        self.wealth = 0.0 #random.gauss(cit_wealth_mean, cit_wealth_sd)
        self.power = 0

    def setUID(): self.UID = x
    def setIdeo(self,x): self.ideo = x
    def setWealth(self,x): self.wealth = x
    def setPower(self,x): self.power = x
    def setStakeholder(self,x): self.stakeholder = x
    def setColor(self,x): self.color = x
    def setXCor(self,x): self.xcor = x
    def setYCor(self,x): self.ycor = x
    def setHidden(self,x): self.hidden = x

    def getUID(self): return self.UID
    def getIdeo(self): return self.ideo
    def getWealth(self): return self.wealth
    def getPower(self): return self.power
    def getStakeholder(self,x): return self.stakeholder
    def getColor(self): return self.color
    def getXCor(self): return self.xcor
    def getYCor(self): return self.ycor
    def getHidden(self): return self.hidden