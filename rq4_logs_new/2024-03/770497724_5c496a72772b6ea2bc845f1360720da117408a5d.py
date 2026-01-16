import numpy as np
import math
from scipy.optimize import fsolve
import random as rnd


class Fluid:
    def __init__(self, mu=0.00089, rho=1000):
        self.mu = mu
        self.rho = rho
        self.nu = mu / rho


class Node:
    def __init__(self, Name='a', Pipes=[], ExtFlow=0):
        self.name = Name
        self.pipes = Pipes
        self.extFlow = ExtFlow

    def getNetFlowRate(self):
        Qtot = self.extFlow
        for p in self.pipes:
            Qtot += p.getFlowIntoNode(self.name)
        return Qtot


class Loop:
    def __init__(self, Name='A', Pipes=[]):
        self.name = Name
        self.pipes = Pipes

    def getLoopHeadLoss(self):
        deltaP = 0
        startNode = self.pipes[0].startNode
        for p in self.pipes:
            phl = p.getFlowHeadLoss(startNode)
            deltaP += phl
            startNode = p.endNode if startNode != p.endNode else p.startNode
        return deltaP


class Pipe:
    def __init__(self, Start='A', End='B', L=100, D=200, r=0.00025, fluid=Fluid()):
        self.startNode = min(Start, End)
        self.endNode = max(Start, End)
        self.length = L
        self.r = r
        self.fluid = fluid
        self.d = D / 1000.0
        self.relrough = self.r / self.d
        self.A = math.pi / 4.0 * self.d ** 2
        self.Q = 10
        self.vel = self.V()
        self.reynolds = self.Re()

    def V(self):
        self.vel = (self.Q / 1000) / self.A
        return self.vel

    def Re(self):
        self.reynolds = (self.fluid.rho * self.V() * self.d) / self.fluid.mu
        return self.reynolds

    def FrictionFactor(self):
        Re = self.Re()
        rr = self.relrough

        def CB():
            cb = lambda f: 1 / (f ** 0.5) + 2.0 * np.log10(rr / 3.7 + 2.51 / (Re * f ** 0.5))
            result = fsolve(cb, 0.01)
            return result[0]

        def lam():
            return 64 / Re

        if Re >= 4000:
            return CB()
        if Re <= 2000:
            return lam()
        CBff = CB()
        Lamff = lam()
        mean = Lamff + ((Re - 2000) / (4000 - 2000)) * (CBff - Lamff)
        sig = 0.2 * mean
        return rnd.normalvariate(mean, sig)

    def frictionHeadLoss(self):
        g = 9.81
        ff = self.FrictionFactor()
        hl = ff * (self.length / self.d) * (self.vel ** 2 / (2 * g))
        return hl

    def getFlowHeadLoss(self, s):
        nTraverse = 1 if s == self.startNode else -1
        nFlow = 1 if self.Q >= 0 else -1
        return nTraverse * nFlow * self.frictionHeadLoss()

    def Name(self):
        return self.startNode + '-' + self.endNode

    def oContainsNode(self, node):
        return self.startNode == node or self.endNode == node

    def printPipeFlowRate(self):
        print(f'The flow in segment {self.Name()} is {self.Q:.2f} L/s')

    def getFlowIntoNode(self, n):
        if n == self.startNode:
            return -self.Q
        return self.Q


class PipeNetwork:
    def __init__(self, Pipes=[], Loops=[], Nodes=[], fluid=Fluid()):
        self.loops = Loops
        self.nodes = Nodes
        self.Fluid = fluid
        self.pipes = Pipes

    def findFlowRates(self):
        N = len(self.nodes) + len(self.loops)
        Q0 = np.full(N, 10)

        def fn(q):
            for i in range(len(self.pipes)):
                self.pipes[i].Q = q[i]
            L = self.getNodeFlowRates()
            L += self.getLoopHeadLosses()
            return L

        FR = fsolve(fn, Q0)
        return FR

