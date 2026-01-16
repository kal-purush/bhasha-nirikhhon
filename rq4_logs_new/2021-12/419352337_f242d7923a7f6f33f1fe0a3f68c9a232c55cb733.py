import math

import Element
import Grid
import Parameters
import Parameters


class HbcSolver():
    uni_ele = None
    data = None
    sides = None

    def __init__(self, data ,npc:int):
        self.uni_ele = Element.Element4_2D(npc)
        self.data = data
        self.sides = []
        self.defineSides()

    def countDetJ(self, pointA, pointB):
        detJ = math.sqrt(pow(pointB[0] - pointA[0], 2) + pow(pointB[1]-pointA[1],2))/2
        return detJ

    def defineSides(self):
        for i in range(len(data)):
            if(i == len(data)-1):
                self.sides.append([self.data[i], self.data[0]])
            else:
                self.sides.append([self.data[i], self.data[i+1]])

    def solveHbc(self):
        for i in range(4):
            N = [[0 for _ in range(4)] for _ in range(self.uni_ele.npc)]
            self.uni_ele.countNmatrix(N, self.uni_ele.borderPoints[i])
            suma = [[] for _ in range(4)]
            detJ = self.countDetJ(self.sides[i][0],self.sides[i][1])
            print(detJ)
            for j in range(4):
                tem = [[] for _ in range(self.uni_ele.npc)]
                for k in range(4):
                    for l in range(self.uni_ele.npc):
                        tem[l].append(25* N[l][j]*N[l][k]*Parameters.Parameters(self.uni_ele.npc).wages[l])
                if self.uni_ele.npc == 2:
                    for a,b,z in zip(tem[0],tem[1], range(4)):
                        suma[z].append((a+b)*detJ)
                else:
                    for a, b, c, z in zip(tem[0], tem[1], tem[2], range(4)):
                        suma[z].append((a + b + c) * detJ)


            print(suma)



if __name__ == "__main__":
    data = [[0,0],
            [0,0.025],
            [0.025,0.025],
            [0,0.025]]
    a = HbcSolver(data, 2)
    a.solveHbc()