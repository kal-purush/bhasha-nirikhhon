class CheckSex:
    def __init__(self):
        self.result = {"correct": "不明", "reason": "不明"}
        
        self.L = [[[1], [1], [1], [0], [0]], "レズビアン"]
        self.G = [[[0], [0], [0], [0], [0]], "ゲイ"]
        self.B = [[[0, 1], [0, 1], [2], [0], [0]], "バイセクシャル"]
        self.T = [[[0, 1], [0, 1], [0, 1, 2, 3, 4], [0, 1, 2], [0, 1]], "トランスジェンダー"] ####
        self.Q = [[[2], [3], [0, 1, 2, 3, 4], [0, 1, 2], [0, 1]], "クィア"] ####
        
        self.ALL = [self.L, self.G, self.B, self.T, self.Q]
        
    def sexClassification(self, same, i):
        print(same, i)
        #バイセクシャル
        if same[0] == same[1] and same[2] == self.B[0][2]:
            self.result = {"correct": self.B[1], "reason": same}
        #クィア
        elif same[0] == self.Q[0][0][0] and same[1] == self.Q[0][1][0]:
            self.result = {"correct": self.Q[1], "reason": same}
        #トランスジェンダー 一番下
        elif same[0] != same[1]:
            self.result = {"correct": self.T[1], "reason": same}
        else:
            self.result = {"correct": i[1], "reason": same}

    def checkSex(self, data):
        for i in self.ALL:
            count = 0
            same = []
            for j in i[0]: #[[ここ]]
                for k in j:
                    if k == data[count]:
                        same.append(k)
                        break
                count += 1
            if len(same) == len(i[0]):
                self.sexClassification(same, i)
                break
                
        return self.result
    
class AutoCheckFromJSON:
    def __init__(self):
        self.CheckSex = CheckSex()
        self.Answer = []
        self.error = {}
        pass
    
    def checkAnswer(self, datas):
        if len(datas) == 0 or len(datas) > 5:
            self.error = {"error": "データが不正です。", "reason": len(datas)}
            return True
        return
    
    def createAnswer(self, datas):
        for data in datas:
            self.Answer.append(datas[data])
        return self.Answer
    
    def autoCheck(self, datas):
        if self.checkAnswer(datas): return self.error
        return self.CheckSex.checkSex(self.createAnswer(datas))