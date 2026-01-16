#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import copy,traceback,time,random,timeit
now_str=lambda: time.strftime("%H:%M:%S",time.localtime())
class Board():
    INITBOARD_LST=['R','N','B','Q','K','B','N','R', 
                   'P','P','P','P','P','P','P','P', 
                   ' ',' ',' ',' ',' ',' ',' ',' ', 
                   ' ',' ',' ',' ',' ',' ',' ',' ', 
                   ' ',' ',' ',' ',' ',' ',' ',' ', 
                   ' ',' ',' ',' ',' ',' ',' ',' ', 
                   'p','p','p','p','p','p','p','p', 
                   'r','n','b','q','k','b','n','r']
    POINTS={"R":5,"N":3,"B":3,"Q":9,"P":1,"K":200," ":0,
            "r":-5,"n":-3,"b":-3,"q":-9,"p":-1,"k":-200}
    DICTCOL={0:"a",1:"b",2:"c",3:"d",4:"e",5:"f",6:"g",7:"h"}
    DICTCOL2={"a":0,"b":1,"c":2,"d":3,"e":4,"f":5,"g":6,"h":7}
    DICTROW={0:"8",1:"7",2:"6",3:"5",4:"4",5:"3",6:"2",7:"1"}
    NMOVES=((1,2),(2,1),(-2,1),(1,-2),(2,-1),(-1,2),(-2,-1),(-1,-2))
    KMOVES=((1,1),(-1,-1),(1,0),(0,1),(-1,0),(0,-1),(1,-1),(-1,1))
    INITMOVED={"k":False,"K":False,"ar":False,"hr":False,"aR":False,"hR":False}
    ALLMOVED={"k":True,"K":True,"ar":True,"hr":True,"aR":True,"hR":True}
    LOGLEVEL={0:"DEBUG",1:"INFO",2:"WARN",3:"ERR",4:"FATAL"}
    
    def __init__(self,board,movestr):
        """turn: True for upper and vice versa"""
        self.board=board
        self.movestr=movestr
        self.genControlMap()
    def getInitBoard():
        b=Board(copy.copy(Board.INITBOARD_LST),"init")
        b.turn=False
        b.moved=copy.copy(Board.INITMOVED)
        b.kloc=[7*8+4,4]
        return b
    def calckloc(self):
        self.kloc=[0,0]
        for i in range(64):
            if self.board[i]=="k":
                self.kloc[0]=i
            if self.board[i]=="K":
                self.kloc[1]=i

    def process_bar(self,p,n=50):
        m=int(p*n)
        s='[%s>%s]%.1f%%\r'%('='*m,'-'*max(n-m-1,0),p*100)
        print(s,end="")
    def log(self,msg,l=1,end="\n"):
        if l<=-1:
            return
        st=traceback.extract_stack()[-2]
        lstr=Board.LOGLEVEL[l]
        if l<3:
            tempstr="%s<%s:%d,%s> %s%s"%(now_str(),st.name,st.lineno,lstr,str(msg),end)
        else:
            tempstr="%s<%s:%d,%s> %s:\n%s%s"%(now_str(),st.name,st.lineno,lstr,str(msg),traceback.format_exc(limit=2),end)
        print(tempstr,end="")
        if l>=2:
            with open("chess_record.txt","a") as f:
                f.write(tempstr)
    def get_best_moves(self):
        s=[]
        next=self
        while True:
            s.append(next.movestr)
            if hasattr(next,"decs") and len(next.decs)>0:
                next=next.decs[0]
            else:
                break
        return ",".join(s)
    def dump(self):
        #self.log(self)
        if not hasattr(self,"score"):
            self.scoreMyself()
        s="\nscore: %.2f, "%(self.score)
        if hasattr(self,"minmax_score"):
            s+="minmax_score: %.2f"%(self.minmax_score)
        else:
            s+="minmax_score: None"
        s+="\nturn: %s"%(self.turn)
        s+="\nbest moves: %s"%(self.get_best_moves())
        s+="\nmoved: %s"%(self.moved)
        s+="\nlegalops: %s"%(",".join([i.movestr for i in self.breed()]),)
        self.log(s)

    def test_genCM():
        TSTBOARD=[" "," "," ","Q","K","B","N","R",
                  "P"," "," "," ","P"," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  " "," ","P"," "," "," "," "," ",
                  " "," "," "," "," "," "," ","p",
                  "r","n","b","q","k"," "," "," "]
        b=Board(copy.copy(TSTBOARD),True,"test_genCM")
        b.log(b)
        b.genControlMap()
        b.log(b.str_CM())
        b.log(b.ControlNum)
    def str_CM(self):
        s=""
        for row in range(8):
            s+="\n   ------------------------\n"
            s+=" %s "%(8-row)
            for col in range(8):
                if self.ControlMap[0][row*8+col]:
                    s+=" x "
                else:
                    s+="   "
        s+="\n   ------------------------"
        for row in range(8):
            s+="\n   ------------------------\n"
            s+=" %s "%(8-row)
            for col in range(8):
                if self.ControlMap[1][row*8+col]:
                    s+=" x "
                else:
                    s+="   "
        s+="\n   ------------------------"
        s+="\n    a  b  c  d  e  f  g  h "
        return s
    def genControlMap(self):
        self.ControlMap=([False]*64,[False]*64)
        for row,col in Board.iter8x8():
            p=self.board[row*8+col]
            if p==" ":
                continue
            elif p=="R":
                self.controlAsR(row,col,True)
            elif p=="r":
                self.controlAsR(row,col,False)
            elif p=="B":
                self.controlAsB(row,col,True)
            elif p=="b":
                self.controlAsB(row,col,False)
            elif p=="Q":
                self.controlAsR(row,col,True)
                self.controlAsB(row,col,True)
            elif p=="q":
                self.controlAsR(row,col,False)
                self.controlAsB(row,col,False)
            elif p=="P":
                self.controlAsUpperP(row,col)
            elif p=="p":
                self.controlAsLowerP(row,col)
            elif p=="N":
                for i,j in Board.NMOVES:
                    if 0<=row+i<8 and 0<=col+j<8 and (self.board[(row+i)*8+col+j]==" " or self.board[(row+i)*8+col+j].islower()==True):
                        self.ControlMap[1][(row+i)*8+col+j]=True
            elif p=="n":
                for i,j in Board.NMOVES:
                    if 0<=row+i<8 and 0<=col+j<8 and (self.board[(row+i)*8+col+j]==" " or self.board[(row+i)*8+col+j].islower()==False):
                        self.ControlMap[0][(row+i)*8+col+j]=True
            elif p=="K":
                for i,j in Board.KMOVES:
                    if 0<=row+i<8 and 0<=col+j<8 and (self.board[(row+i)*8+col+j]==" " or self.board[(row+i)*8+col+j].islower()==True):
                        self.ControlMap[1][(row+i)*8+col+j]=True
            elif p=="k":
                for i,j in Board.KMOVES:
                    if 0<=row+i<8 and 0<=col+j<8 and (self.board[(row+i)*8+col+j]==" " or self.board[(row+i)*8+col+j].islower()==False):
                        self.ControlMap[0][(row+i)*8+col+j]=True
            else:
                self.log("sth wired happens",l=2) 
        self.ControlNum=[sum([int(j) for j in i]) for i in self.ControlMap]
    def test_score():
        TSTBOARD=([" "," "," ","Q","K","B","N","R"],
                  ["P"," "," "," ","P"," "," "," "],
                  [" "," "," "," "," "," "," "," "],
                  [" "," "," "," "," "," "," "," "],
                  [" "," "," "," "," "," "," "," "],
                  [" "," ","P"," "," "," "," "," "],
                  [" "," "," "," "," "," "," ","p"],
                  ["r","n","b","q","k"," "," "," "])
        b=Board(copy.copy(Board.INITBOARD_STR),"test_score")
        b.scoreMyself()
        #b=Board(TSTBOARD,True,"test_score")
        b.log(b)
    def scoreMyself(self):
        self.score=0
        for i in self.board:
            self.score+=Board.POINTS[i]
        #self.score+=(self.ControlNum[1]-self.ControlNum[0])/10.0

    def test_puning():
        TSTBOARD=[" "," "," "," "," "," "," ","K",
                  " "," "," "," "," "," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  "R"," "," "," "," "," "," "," ",
                  " ","R"," "," "," "," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  " "," "," "," "," "," "," ","k"]
        TS2BOARD=[" "," "," "," "," "," "," ","k",
                  " "," "," "," "," "," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  " ","r"," "," "," "," "," "," ",
                  "r"," "," "," "," "," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  " "," "," "," "," "," "," ","K"]
        #white2,674
        TS3BOARD=[" "," ","r"," ","R"," "," ","K",
                  " "," "," "," "," "," ","B","P",
                  " "," "," "," "," "," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  " ","P"," ","b"," "," "," "," ",
                  "Q","p","P"," "," "," "," "," ",
                  " "," ","p"," ","R"," "," "," ",
                  " ","k"," "," "," ","q"," "," "]
        #white3,687
        TS4BOARD=["R"," "," "," "," "," "," ","R",
                  " "," "," "," ","P","P","K"," ",
                  " "," "," "," ","B"," ","P"," ",
                  " "," "," "," ","p"," ","p"," ",
                  " "," "," "," "," "," "," "," ",
                  "Q","p","P"," "," "," "," "," ",
                  " "," ","p"," "," "," "," ","r",
                  " ","k"," "," "," ","q"," ","r"]
        #做对了，会躲
        TS5BOARD=["r"," "," ","n","k"," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  " ","N"," "," "," "," "," "," ",
                  " "," "," "," ","r"," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  " "," "," "," "," "," ","N","K"]
        deepth=4
        #b=Board(copy.copy(TS4BOARD),"test_puning")
        #b.turn=False
        #b.moved=Board.ALLMOVED
        #b.calckloc()
        b=Board.getInitBoard()
        #b.turn=True;

        tik=time.time()
        ax=b.alpha_beta_puning(deepth,-500,500,deepth)
        tok=time.time()
        
        b.log("finish %d cases in %.2fms at deepth=%d"%(ax,(tok-tik)*1000,deepth))
        b.log(b)
        next=b.decs[0]
        while True:
            #b.log(next)
            b.log(next.movestr)
            if hasattr(next,"decs") and len(next.decs)>0:
                next=next.decs[0]
            else:
                break
    def alpha_beta_puning(self,deep,alpha,beta,thedeep):
        ax=0
        if thedeep==deep:
            ntol=int(9.79**(thedeep+1))
        if deep==0:
            self.scoreMyself()
            self.minmax_score=self.score
            return 1
        else:
            self.decs=[]
            if self.turn:
                for d in self.breed():
                    if thedeep==deep:
                        self.process_bar(ax/ntol)
                    ax+=1
                    ax+=d.alpha_beta_puning(deep-1,alpha,beta,thedeep)
                    eval=d.minmax_score
                    self.decs.append(d)
                    alpha=max(alpha,eval)
                    if beta<=alpha:
                        pass
                        #break
                if len(self.decs)>0:
                    self.decs.sort(key=lambda d:d.minmax_score,reverse=self.turn)
                    self.minmax_score=self.decs[0].minmax_score
                else:
                    if self.ControlMap[0][self.kloc[1]]:
                        self.scoreMyself()
                        self.minmax_score=self.score-Board.POINTS["K"]
                    else:
                        self.decs.append(Board([" "]*64,"draw"))
                        self.minmax_score=0
            else:
                #self.log("lower's turn at deep=%d"%(deep))
                for d in self.breed():
                    if thedeep==deep:
                        self.process_bar(ax/ntol)
                    ax+=1
                    ax+=d.alpha_beta_puning(deep-1,alpha,beta,thedeep)
                    eval=d.minmax_score
                    self.decs.append(d)
                    beta=min(beta,eval)
                    if beta<=alpha:
                        pass
                        #break
                if len(self.decs)>0:
                    self.decs.sort(key=lambda d:d.minmax_score,reverse=self.turn)
                    self.minmax_score=self.decs[0].minmax_score
                else:
                    if self.ControlMap[1][self.kloc[0]]:
                        self.scoreMyself()
                        self.minmax_score=self.score-Board.POINTS["k"]
                        #self.log("Upper win")
                    else:
                        self.decs.append(Board([" "]*64,"draw"))
                        self.minmax_score=0
        return ax
    def getMove(self,mv=""):
        board=self
        while True:
            legalops=[i for i in board.breed()]
            self.log("Your move: ",end="")
            mv=input()
            try:
                if mv.startswith("back"):
                    self.log("you son of a bitch",l=2)
                    board=board.father.father
                    board.log(board)
                    continue
                elif mv.startswith("dump"):
                    self.dump()
                    continue
                for d in legalops:
                    if d.movestr==mv:
                        d.father=board
                        if hasattr(d.father,"decs") and len(d.father.decs)>1:
                            for d2 in d.father.decs:
                                if d2!=d:
                                    del d2
                        return d,mv
                else:
                    self.log("wrong move: illigel")
            except:
                self.log("wrong move",l=3)
    def play(deepth=4):
        #TSTBOARD=[" "," "," "," "," "," ","K"," ",
        #          " "," "," "," "," "," "," "," ",
        #          " "," "," "," "," "," "," "," ",
        #          " "," "," "," "," "," "," "," ",
        #          " "," "," "," "," "," "," "," ",
        #          " "," "," "," "," "," "," ","R",
        #          " "," "," "," "," "," ","R"," ",
        #          " "," ","k"," "," "," "," "," "]
        #b=Board(TSTBOARD,"test")
        #b.turn=False
        #b.moved=Board.ALLMOVED
        #b.calckloc()
        b=Board.getInitBoard()
        b.log("begin a new game with %s"%(input("please tell me your name: ")),l=2)
        while True:
            b.log(b)
            b,movestr=b.getMove()
            tik=time.time()
            ax=b.alpha_beta_puning(deepth,-500,500,deepth)
            tok=time.time()
            if hasattr(b,"decs") and len(b.decs)>0:
                b.decs[0].father=b
                for d in b.decs[1:]:
                    del d
                b=b.decs[0]
                b.log("%s %s considered %d cases in %.2fms at deepth=%d"%(movestr,b.movestr,ax,(tok-tik)*1000,deepth),l=2)
                if b.minmax_score>=100:
                    b.log(b)
                    b.log("I win: %s"%(b.get_best_moves()),l=2)
                    break
            else:
                if b.ControlMap[0][b.kloc[1]]:
                    b.log("you win",l=2)
                else:
                    b.log("draw: no piece can move",l=2)
                break
        b.log("press any key to quit",end="")
        input()
    def test_breed():
        TSTBOARD=[" "," "," "," "," "," "," ","K",
                  " "," "," "," "," "," "," "," ",
                  " ","r"," ","n","b"," "," "," ",
                  " "," "," ","P"," ","P"," "," ",
                  " "," ","p"," ","p"," "," "," ",
                  " ","p"," "," "," "," "," "," ",
                  "p","p","p"," "," "," ","k"," ",
                  " "," "," "," "," "," "," "," "]
        TS2BOARD=[" "," "," "," "," "," "," ","K",
                  " "," "," "," "," ","p"," "," ",
                  " "," "," "," ","p"," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  " "," "," "," "," "," "," "," ",
                  " "," "," "," ","P"," "," "," ",
                  " "," "," "," "," ","P"," "," ",
                  " "," "," "," "," "," "," ","k"]
        b=Board(copy.copy(TSTBOARD),"test_breed")
        b.turn=False
        b.moved=Board.ALLMOVED
        b.calckloc()
        ax=0
        for dec in b.breed():
            b.log(dec.movestr)
            dec.scoreMyself()
            ax+=1
        b.log("ax=%d"%(ax))
    def test_En_passant():
        TS2BOARD=([" "," "," "," "," "," "," "," "],
                  [" "," "," "," "," "," "," "," "],
                  [" "," "," "," "," "," "," "," "],
                  [" "," "," "," "," "," "," "," "],
                  ["p","P"," "," "," "," "," "," "],
                  [" "," "," "," "," "," "," "," "],
                  [" "," "," "," "," "," "," "," "],
                  [" "," "," "," "," "," "," "," "])
        b=Board(TS2BOARD,True,"init")
        b.breed(0,timeit=True)
    def test_Castling():
        TS2BOARD=(["R"," "," "," ","K"," "," ","R"],
                  [" "," "," "," "," "," "," "," "],
                  [" "," "," "," "," "," "," "," "],
                  [" "," "," "," "," "," "," "," "],
                  [" "," "," "," "," "," "," "," "],
                  [" "," "," "," "," "," "," "," "],
                  [" "," "," "," "," "," "," "," "],
                  [" "," "," ","r"," "," "," "," "])
        b=Board(TS2BOARD,"init")
        b.turn=True
        #b.moved={"k":True,"K":False,"ar":True,"hr":True,"aR":False,"hR":False}
        #b.moved={"k":False,"K":False,"ar":False,"hr":False,"aR":True,"hR":True}
        b.moved=Board.INITMOVED
        b.breed(0,timeit=True)
    def breed(self):
        """turn: True for upper, False for lower"""
        oppo=int(not self.turn)
        me=int(self.turn)
        for row,col in Board.iter8x8():
            p=self.board[row*8+col]
            if p==" " or p.islower()==self.turn:
                continue
            if p=="R" or p=="r":
                for dec in self.moveAsR(row,col):
                    if not dec.ControlMap[oppo][dec.kloc[me]]:
                    #if True:
                        yield dec
            elif p.upper()=="B":
                for dec in self.moveAsB(row,col):
                    #if True:
                    if not dec.ControlMap[oppo][dec.kloc[me]]:
                        yield dec
            elif p.upper()=="Q":
                for dec in self.moveAsR(row,col):
                    if not dec.ControlMap[oppo][dec.kloc[me]]:
                    #if True:
                        yield dec
                for dec in self.moveAsB(row,col):
                    if not dec.ControlMap[oppo][dec.kloc[me]]:
                    #if True:
                        yield dec
            elif p.upper()=="N":
                for i,j in Board.NMOVES:
                    if 0<=row+i<8 and 0<=col+j<8 and (self.board[(row+i)*8+col+j]==" " or self.board[(row+i)*8+col+j].islower()==self.turn):
                        dec=self.move(row,col,row+i,col+j)
                        if not dec.ControlMap[oppo][dec.kloc[me]]:
                        #if True:
                            yield dec
            elif p=="K":
                for dec in self.moveAsUpperK(row,col):
                    if not dec.ControlMap[oppo][dec.kloc[me]]:
                    #if True:
                        yield dec
            elif p=="k":
                for dec in self.moveAsLowerK(row,col):
                    if not dec.ControlMap[oppo][dec.kloc[me]]:
                    #if True:
                        yield dec
            elif p=="P":
                for dec in self.moveAsUpperP(row,col):
                    #if True:
                    if not dec.ControlMap[oppo][dec.kloc[me]]:
                        yield dec
            elif p=="p":
                for dec in self.moveAsLowerP(row,col):
                    #if True:
                    if not dec.ControlMap[oppo][dec.kloc[me]]:
                        yield dec
            else:
                self.log("sth wired happens",l=4)
    def moveAsUpperK(self,row,col):
        oppo=0;me=1
        if self.moved["K"]==False and self.moved["hR"]==False\
            and self.board[5]==" " and self.board[6]==" "\
            and not self.ControlMap[oppo][4] and not self.ControlMap[oppo][5]\
            and not self.ControlMap[oppo][6] and not self.ControlMap[oppo][7]:
            dec=self.moveCastling(0,4,0,6)
            dec.moved["K"]=True
            dec.moved["hR"]=True
            dec.kloc[me]=6
            yield dec
        if self.moved["K"]==False and self.moved["aR"]==False\
            and self.board[1]==" " and self.board[2]==" " and self.board[3]==" "\
            and not self.ControlMap[oppo][0] and not self.ControlMap[oppo][1] and not self.ControlMap[0][2]\
            and not self.ControlMap[oppo][3] and not self.ControlMap[oppo][4]:
            dec=self.moveCastling(0,4,0,2)
            dec.moved["K"]=True
            dec.moved["aR"]=True
            dec.kloc[me]=2
            yield dec
        for i,j in Board.KMOVES:
            if 0<=row+i<8 and 0<=col+j<8 and (self.board[(row+i)*8+col+j]==" " or self.board[(row+i)*8+col+j].islower()==self.turn):
                dec=self.move(row,col,row+i,col+j)
                if not dec.ControlMap[oppo][(row+i)*8+col+j]:
                    dec.moved["K"]=True
                    dec.kloc[1]=((row+i)*8+col+j)
                    yield dec
    def moveAsLowerK(self,row,col):
        oppo=1;me=0
        if self.moved["k"]==False and self.moved["hr"]==False\
            and self.board[7*8+5]==" " and self.board[7*8+6]==" "\
            and not self.ControlMap[oppo][7*8+4] and not self.ControlMap[oppo][7*8+5]\
            and not self.ControlMap[oppo][7*8+6] and not self.ControlMap[oppo][7*8+7]:
            dec=self.moveCastling(7,4,7,6)
            dec.moved["k"]=True
            dec.moved["hr"]=True
            dec.kloc[me]=7*8+6
            yield dec
        if self.moved["k"]==False and self.moved["ar"]==False\
            and self.board[7*8+1]==" " and self.board[7*8+2]==" " and self.board[7*8+3]==" "\
            and not self.ControlMap[oppo][7*8] and not self.ControlMap[oppo][7*8+1] and not self.ControlMap[oppo][7*8+2]\
            and not self.ControlMap[oppo][7*8+3] and not self.ControlMap[oppo][7*8+4]:
            dec=self.moveCastling(7,4,7,2)
            dec.moved["k"]=True
            dec.moved["ar"]=True
            dec.kloc[me]=7*8+2
            yield dec
        for i,j in Board.KMOVES:
            if 0<=row+i<8 and 0<=col+j<8 and (self.board[(row+i)*8+col+j]==" " or self.board[(row+i)*8+col+j].islower()==self.turn):
                dec=self.move(row,col,row+i,col+j)
                if not dec.ControlMap[oppo][(row+i)*8+col+j]:
                    dec.moved["k"]=True
                    dec.kloc[me]=(row+i)*8+col+j
                    yield dec
    def moveCastling(self,row1,col1,row2,col2):
        neoboard=copy.copy(self.board)
        neoboard[row1*8+4]=" "
        neoboard[row1*8+col2]=self.board[row1*8+4]
        if col2>col1:
            movestr="O-O"
            neoboard[row1*8+7]=" "
            neoboard[row1*8+5]=self.board[row1*8+7]
        else:
            movestr="O-O-O"
            neoboard[row1*8]=" "
            neoboard[row1*8+3]=self.board[row1*8]
        return self.produce(neoboard,movestr)
    def moveAsUpperP(self,row,col):
        #if row==4:
        #    if col!=7 and self.board[row*8+col+1]=="p":
        #        colplus=Board.DICTCOL[col+1]
        #        if self.movestr=="p%s2-%s4"%(colplus,colplus):
        #            yield self.moveEnPassant(row,col,row+1,col+1)
        #    if col!=0 and self.board[row*8+col-1]=="p":
        #        colminus=Board.DICTCOL[col-1]
        #        if self.movestr=="p%s2-%s4"%(colminus,colminus):
        #            yield self.moveEnPassant(row,col,row+1,col-1)
        if self.board[(row)*8+8+col]==" ":
            if row<6:
                yield self.move(row,col,row+1,col)
            else:
                for d in self.movePromotion(row,col,7,col):
                    yield d
            if row==1 and self.board[3*8+col]==" ":
                yield self.move(row,col,3,col)
        if col!=7 and self.board[(row)*8+col+9]!=" " and self.board[row*8+col+9].islower()==self.turn:
            if row<6:
                yield self.move(row,col,row+1,col+1)
            else:
                for d in self.movePromotion(row,col,7,col+1,"x"):
                    yield d
        if col!=0 and self.board[row*8+col+7]!=" " and self.board[row*8+col+7].islower()==self.turn:
            if row<6:
                yield self.move(row,col,row+1,col-1)
            else:
                for d in self.movePromotion(row,col,7,col-1,"x"):
                    yield d
    def controlAsUpperP(self,row,col):
        if col!=7:
            self.ControlMap[1][row*8+col+9]=True 
        if col!=0:
            self.ControlMap[1][row*8+col+7]=True
    def moveAsLowerP(self,row,col):
        #jump & En passant
        #if row==3:
        #    if col!=7 and self.board[row*8+col+1]=="P":
        #        colplus=Board.DICTCOL[col+1]
        #        if self.movestr=="P%s7-%s5"%(colplus,colplus):
        #            yield self.moveEnPassant(row,col,row-1,col+1)
        #    if col!=0 and self.board[row*8+col-1]=="P":
        #        colminus=Board.DICTCOL[col-1]
        #        if self.movestr=="P%s7-%s5"%(colminus,colminus):
        #            yield self.moveEnPassant(row,col,row-1,col-1)
        
        if self.board[(row-1)*8+col]==" ":
            if row>1:
                yield self.move(row,col,row-1,col)
            else:
                for d in self.movePromotion(row,col,0,col):
                    yield d
            if row==6 and self.board[4*8+col]==" ":
                yield self.move(row,col,4,col)
        if col!=7 and self.board[row*8+col-7]!=" " and self.board[row*8+col-7].islower()==self.turn:
            if row>1:
                yield self.move(row,col,row-1,col+1)
            else:
                for d in self.movePromotion(row,col,0,col+1,"x"):
                    yield d      
        if col!=0 and self.board[row*8+col-9]!=" " and self.board[row*8+col-9].islower()==self.turn:
            if row>1:
                yield self.move(row,col,row-1,col-1)
            else:
                for d in self.movePromotion(row,col,0,col-1,"x"):
                    yield d
    def controlAsLowerP(self,row,col):
        if col!=7:
            self.ControlMap[0][row*8+col-7]=True 
        if col!=0:
            self.ControlMap[0][row*8+col-9]=True
    def movePromotion(self,row,col,row2,col2,dash="-"):
        if self.turn:
            for p2 in "RNBQ":
                neoboard=copy.copy(self.board)
                neoboard[row*8+col]=" "
                neoboard[row2*8+col2]=p2
                movestr="P%s%s%s%s%s=%s"%(Board.DICTCOL[col],Board.DICTROW[row],dash,Board.DICTCOL[col2],Board.DICTROW[row2],p2)
                yield self.produce(neoboard,movestr)
        else:
            for p2 in "rnbq":
                neoboard=copy.copy(self.board)
                neoboard[row*8+col]=" "
                neoboard[row2*8+col2]=p2
                movestr="p%s%s%s%s%s=%s"%(Board.DICTCOL[col],Board.DICTROW[row],dash,Board.DICTCOL[col2],Board.DICTROW[row2],p2)
                yield self.produce(neoboard,movestr)
    def moveEnPassant(self,row1,col1,row2,col2):
        neoboard=copy.copy(self.board)
        neoboard[row1*8+col2]=" "
        neoboard[row1*8+col1]=" "
        neoboard[row2*8+col2]=self.board[row1*8+col1]
        movestr="%s%s%sx%s%s"%(self.board[row1*8+col1],Board.DICTCOL[col1],Board.DICTROW[row1],Board.DICTCOL[col2],Board.DICTROW[row2])
        return self.produce(neoboard,movestr)
    def moveAsB(self,row,col):
        for i in range(1,8):
            row2=row-i
            col2=col-i
            if col2<0 or row2<0 or (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].isupper()==self.turn):
                break
            else:
                yield self.move(row,col,row2,col2)
                #if (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].islower()==self.turn):
                if self.board[row2*8+col2]!=" ":
                    break
        for i in range(1,8):
            row2=row+i
            col2=col+i
            if col2>=8 or row2>=8 or (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].isupper()==self.turn):
                break
            else:
                yield self.move(row,col,row2,col2)
                #if (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].islower()==self.turn):
                if self.board[row2*8+col2]!=" ":
                    break
        for i in range(1,8):
            row2=row-i
            col2=col+i
            if col2>=8 or row2<0 or (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].isupper()==self.turn):
                break
            else:
                yield self.move(row,col,row2,col2)
                #if (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].islower()==self.turn):
                if self.board[row2*8+col2]!=" ":
                    break
        for i in range(1,8):
            row2=row+i
            col2=col-i
            if col2<0 or row2>=8 or (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].isupper()==self.turn):
                break
            else:
                yield self.move(row,col,row2,col2)
                #if (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].islower()==self.turn):
                if self.board[row2*8+col2]!=" ":
                    break
    def controlAsB(self,row,col,turn):
        cMap=self.ControlMap[int(turn)]
        for i in range(1,8):
            row2=row-i
            col2=col-i
            if col2<0 or row2<0 or (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].isupper()==turn):
                break
            else:
                cMap[row2*8+col2]=True
                if (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].islower()==turn):
                    break
        for i in range(1,8):
            row2=row+i
            col2=col+i
            if col2>=8 or row2>=8 or (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].isupper()==turn):
                break
            else:
                cMap[row2*8+col2]=True
                if (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].islower()==turn):
                    break
        for i in range(1,8):
            row2=row-i
            col2=col+i
            if col2>=8 or row2<0 or (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].isupper()==turn):
                break
            else:
                cMap[row2*8+col2]=True
                if (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].islower()==turn):
                    break
        for i in range(1,8):
            row2=row+i
            col2=col-i
            if col2<0 or row2>=8 or (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].isupper()==turn):
                break
            else:
                cMap[row2*8+col2]=True
                if (self.board[row2*8+col2]!=" " and self.board[row2*8+col2].islower()==turn):
                    break
    def moveAsR(self,row,col):
        for i in range(1,8):
            col2=col-i
            if col2<0 or (self.board[row*8+col2]!=" " and self.board[row*8+col2].isupper()==self.turn):
                break
            else:
                dec=self.move(row,col,row,col2)
                if row==0:
                    if col==0 and self.board[0]=="R" and self.moved["aR"]==False:
                        dec.moved["aR"]=True
                    elif col==7 and self.board[7]=="R" and self.moved["hR"]==False:
                        dec.moved["hR"]=True
                elif row==7:
                    if col==0 and self.board[7*8]=="r" and self.moved["ar"]==False:
                        dec.moved["ar"]=True
                    elif col==7 and self.board[7*8+7]=="r" and self.moved["hr"]==False:
                        dec.moved["hr"]=True
                yield dec
                if (self.board[row*8+col2]!=" " and self.board[row*8+col2].islower()==self.turn):
                    break
        for i in range(1,8):
            col2=col+i
            if col2>=8 or (self.board[row*8+col2]!=" " and self.board[row*8+col2].isupper()==self.turn):
                break
            else:
                dec=self.move(row,col,row,col2)
                if row==0:
                    if col==0 and self.board[0]=="R" and self.moved["aR"]==False:
                        dec.moved["aR"]=True
                    elif col==7 and self.board[7]=="R" and self.moved["hR"]==False:
                        dec.moved["hR"]=True
                elif row==7:
                    if col==0 and self.board[7*8]=="r" and self.moved["ar"]==False:
                        dec.moved["ar"]=True
                    elif col==7 and self.board[7*8+7]=="r" and self.moved["hr"]==False:
                        dec.moved["hr"]=True
                yield dec
                if (self.board[row*8+col2]!=" " and self.board[row*8+col2].islower()==self.turn):
                    break
        for i in range(1,8):
            row2=row-i
            if row2<0 or (self.board[row2*8+col]!=" " and self.board[row2*8+col].isupper()==self.turn):
                break
            else:
                dec=self.move(row,col,row2,col)
                if row==0:
                    if col==0 and self.board[0]=="R" and self.moved["aR"]==False:
                        dec.moved["aR"]=True
                    elif col==7 and self.board[7]=="R" and self.moved["hR"]==False:
                        dec.moved["hR"]=True
                elif row==7:
                    if col==0 and self.board[7*8]=="r" and self.moved["ar"]==False:
                        dec.moved["ar"]=True
                    elif col==7 and self.board[7*8+7]=="r" and self.moved["hr"]==False:
                        dec.moved["hr"]=True
                yield dec
                if (self.board[row2*8+col]!=" " and self.board[row2*8+col].islower()==self.turn):
                    break
        for i in range(1,8):
            row2=row+i
            if row2>=8 or (self.board[row2*8+col]!=" " and self.board[row2*8+col].isupper()==self.turn):
                break
            else:
                dec=self.move(row,col,row2,col)
                if row==0:
                    if col==0 and self.board[0]=="R" and self.moved["aR"]==False:
                        dec.moved["aR"]=True
                    elif col==7 and self.board[7]=="R" and self.moved["hR"]==False:
                        dec.moved["hR"]=True
                elif row==7:
                    if col==0 and self.board[7*8]=="r" and self.moved["ar"]==False:
                        dec.moved["ar"]=True
                    elif col==7 and self.board[7*8+7]=="r" and self.moved["hr"]==False:
                        dec.moved["hr"]=True
                yield dec
                if (self.board[row2*8+col]!=" " and self.board[row2*8+col].islower()==self.turn):
                    break
    def controlAsR(self,row,col,turn):
        cMap=self.ControlMap[int(turn)]
        for i in range(1,8):
            col2=col-i
            if col2<0 or (self.board[row*8+col2]!=" " and self.board[row*8+col2].isupper()==turn):
                break
            else:
                cMap[row*8+col2]=True
                if (self.board[row*8+col2]!=" " and self.board[row*8+col2].islower()==turn):
                    break
        for i in range(1,8):
            col2=col+i
            if col2>=8 or (self.board[row*8+col2]!=" " and self.board[row*8+col2].isupper()==turn):
                break
            else:
                cMap[row*8+col2]=True
                if (self.board[row*8+col2]!=" " and self.board[row*8+col2].islower()==turn):
                    break
        for i in range(1,8):
            row2=row-i
            if row2<0 or (self.board[row2*8+col]!=" " and self.board[row2*8+col].isupper()==turn):
                break
            else:
                cMap[row2*8+col]=True
                if (self.board[row2*8+col]!=" " and self.board[row2*8+col].islower()==turn):
                    break
        for i in range(1,8):
            row2=row+i
            if row2>=8 or (self.board[row2*8+col]!=" " and self.board[row2*8+col].isupper()==turn):
                break
            else:
                cMap[row2*8+col]=True
                if (self.board[row2*8+col]!=" " and self.board[row2*8+col].islower()==turn):
                    break
    def produce(self,neoboard,movestr):
        dec=Board(neoboard,movestr)
        dec.turn=not self.turn
        dec.moved=copy.copy(self.moved)
        dec.kloc=copy.copy(self.kloc)
        return dec
    def move(self,row1,col1,row2,col2):
        neoboard=copy.copy(self.board)
        neoboard[row1*8+col1]=" "
        neoboard[row2*8+col2]=self.board[row1*8+col1]
        if self.board[row2*8+col2]==" ":
            dash="-"
        else:
            dash="x"
        movestr="%s%s%s%s%s%s"%(self.board[row1*8+col1],Board.DICTCOL[col1],Board.DICTROW[row1],dash,Board.DICTCOL[col2],Board.DICTROW[row2])
        return self.produce(neoboard,movestr)

    def iter8x8():
        for row in range(8):
            for col in range(8):
                yield row,col    
    def __str__(self):
        s="move: %s"%(self.movestr)
        for row in range(8):
            s+="\n   ------------------------\n"
            s+=" %s  "%(8-row)
            s+="  ".join(list(self.board[row*8:row*8+8]))
        s+="\n   ------------------------"
        s+="\n    a  b  c  d  e  f  g  h "
        return s
if __name__=="__main__":
    #Board.test_breed()
    #Board.test_En_passant()
    #Board.test_Castling()
    #Board.test_genCM()
    #Board.test_score()
    Board.test_puning()
    #Board.play()
    
    