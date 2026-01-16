import numpy as np

class Board(object):
    '''棋盘，默认是“井字棋”Tie Tac Toe，也可以作为五子棋,围棋等黑白棋的棋盘'''
    def __init__(self, height=3, width=3, player='x'):
        #  M x N 矩阵初始化棋盘ox，值是棋子类型，0空，1代表'x'，-1代表'o';  
        self.__ox = np.zeros((height, width), dtype=int )
        self.__next_player = player
        #现棋盘空余可下位置列表如{(0,0), (0,1), }
        self.__free_moves = [(i,j) for i in range(height) for j in range(width)] 
        #下棋的落子顺序
        self.__move_steps = []
        
    
    @property
    def ox(self):
        return self.__ox
    
    @property
    def player(self):
        return self.__next_player
    
    @property
    def moves(self):
        #返回可下位置列表
        return self.__free_moves

    @property
    def steps(self):
        #返回落子的顺序位置，第一个player是X
        return self.__move_steps
    
    
    def show(self):
        '''通过 Q X + 展示棋盘'''
        #'''打印列标号'''
        print("{0:>15}".format('x\y'), end='') # 5个字符
        for j in range(self.__ox.shape[1]):
            print("{0:^5}".format(j), end='') # 5个字符          
        print('\n')  # 换行,打印空行        
      
        #'''打印棋盘'''            
        for i in range(self.__ox.shape[0]):        #行数
            #'''打印行标号'''
            print("{0:>15}".format(i), end='') # 5个字符
            #'''打印该行棋盘'''    
            for j in range(self.__ox.shape[1]):
                if self.__ox[i][j] == 0:    #未落子  5个字符
                    print("{0:^5}".format('+'), end='')
                elif self.__ox[i][j] == 1:   # x 落子  5个字符
                    print("{0:^5}".format('X'), end='')
                elif self.__ox[i][j] == -1:  # o 落子
                    print("{0:^5}".format('Q'), end='') 
            print('\n') 
                    
    
    def move(self, location): 
        '''通过位置更新棋盘，严格的X/Q顺序：玩家使用默认的player，只能输入位置，之后更改player'''
        #落子位置可用则更新棋盘，返回1. 否则返回0
        if location in self.moves:
            self.__ox[location] = 1 if self.__next_player=='x' else -1
            self.__next_player = 'o' if self.__next_player=='x' else 'x'
            self.__free_moves.remove( location ) 
            self.__move_steps.append( location )
            return 1    
        else:
            return 0 
        
     
    def retract_move(self):
        '''毁一步棋严格的X/Q顺序'''
        undo = self.__move_steps.pop()
        self.__free_moves.append(undo)
        self.__ox[undo] = 0 
        self.__next_player = 'o' if self.__next_player=='x' else 'x'    
        
        
    def states(self, ninline=3):  #此处应该考虑ninline要小于等于矩阵行列的最小值
        '''ninline表示连成一线的子数是多少为赢
           返回棋盘棋局胜负标志，>=1代表'x'赢，<=-1代表'o'赢，0代表平局，None代表未结束'''
        
        #生成棋盘所有ninline维方阵，并判断方阵是否有连成一线的情况
        matrixlist = [self.__ox[i:i+ninline, j:j+ninline] 
                      for i in range(self.__ox.shape[0]- ninline +1)
                        for j in range(self.__ox.shape[1]- ninline +1)
                  ]
   
        #计算方阵连线情况,1代表'x'连成一线，-1代表'o'连成一线，0代表没有出现'''
        for nm in matrixlist:
            #把各行,列的和组成集合，查找集合中是否存在ninline或-ninline，代表连成一行'''
            n_line = set(nm.sum(axis=1)) | set(nm.sum(axis=0))
            #第一条对角线的和加入列表；使用方阵的迹'''
            n_line.add(nm.trace())
            #第二条对角线的和加入列表；使用方阵反转后的迹'''
            n_line.add(nm[::-1].trace())
        
            #只要存在连成一线的情况，就不在检查后续。
            states = -1 if -ninline in n_line else 1 if ninline in n_line else 0
            if states == 1 or states == -1 : break

        #有ninline维方阵返回1或者-1，加权可落子处多少，输赢结果'''
        if states == 1 : return 1 + len(self.__free_moves)
        if states == -1 : return -1 - len(self.__free_moves) 
        #所有ninline维方阵返回都是0，检查是否有可落子处，无则返回0代表平局，有则None代表未结束'''    
        return 0 if self.__free_moves == [] else None 
    