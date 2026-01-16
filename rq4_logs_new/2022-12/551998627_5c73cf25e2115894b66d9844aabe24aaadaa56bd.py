class KH:
    def __init__(self, ma, ten, tvao, tra):
        self.ma = ma
        self.ten = ten
        
        vao = [int(x) for x in tvao.split(':')]
        ra = [int(x) for x in tra.split(':')]

        self.gio = ra[0] - vao[0]
        if ra[1] < vao[1]:
            self.gio -= 1
            self .phut = 60 - vao[1]
        else: self.phut = ra[1] - vao[1]
    
    def toString(self):
        return f'{self.ma} {self.ten} {self.gio} gio {self.phut} phut'

ls = []
for i in range(int(input())):
    ls.append((KH(input(), input(), input(), input())))

ls.sort(key = lambda x : (-x.gio, -x.phut))

for x in ls: print(x.toString())