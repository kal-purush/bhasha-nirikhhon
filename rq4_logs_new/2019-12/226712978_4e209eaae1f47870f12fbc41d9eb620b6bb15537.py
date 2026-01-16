
import os
class SGF():
    def __init__(self):
        self.POS = 'abcdefghijklmno'
    def readData(self,path):
        f = open(path,'r')
        data = f.read()
        f.close()
        data = data.split(";")
        board = []
        step =0
        for point in data[2:-1]:
            x = self.POS.index(point[2])
            y = self.POS.index(point[3])
            color = step%2 +1
            step +=1
            board.append([x,y,color,step])
        return board

    def transferDataToTrain(self,path,color):
        data = self.readData(path)
        total_step = len(data)
        train_x = []
        train_y = []
        player = 1.0
        tmp = [0.0 for i in range(225)]
        for step in range(total_step):
            y = [0.0 for i in range(225)]
            train_x.append(tmp.copy())
            tmp[data[step][0]*15+data[step][1]] = player
            player = 2.0 if player == 1.0 else 1.0
            y[data[step][0]*15+data[step][1]] = 1.0
            train_y.append(y.copy())
        return train_x,train_y
    @staticmethod
    def getAllFileName(path):
        root = os.listdir(path)
        files = []
        for p in root:
            child = os.path.join("%s%s" % (path,p))
            files.append(child)
        return files