'''
This python script demonstrates the Half-Space Trees Anomaly detector according to the algorithm defined in the paper by Tang, Ting and Liu in https://www.ijcai.org/Proceedings/11/Papers/254.pdf
HSTs are fast anomaly detectors in online streaming data
HSTs work good when the anomalies are sparse, which is a realistic assumption
github link: 
'''

import numpy as np
import matplotlib.pyplot as plt
import random

#Node Class defined to store each node of the Half-Space Tree (HST)
class Node:
  '''
  lChild: Left child node of HST
  rChild: Right child node of HST
  r: Mass of node in reference window
  l: Mass of node in latest window
  q: Splitting attribute for each dimenion
  p: Midpoint of range in each stream
  k: Depth of node
  '''
  def __init__(self, lChild=None, rChild=None, r=0, l=0, q=0, p=0.0, k=0):
      self.lChild = lChild
      self.rChild = rChild
      self.r = r
      self.l = l
      self.q = q
      self.p = p
      self.k = k

#Half-Space Forest -> Ensemble technique to boost accuracy from each HST
class HST_Forest:
  '''
  wdwSize: Size of reference and latest windows
  nt: No. of HS trees in the forest
  h: Max. depth of each node in the tree
  HSTrees: List of roots of all trees in the forest
  '''
  def __init__(self,wdwSize,nt,h):
    self.wdwSize = wdwSize
    self.nt = nt
    self.h = h
    self.HSTrees = []

  #Generate max and min arrays
  def genMaxMin(self,dim):
      mxArr = np.zeros((dim))
      mnArr = np.zeros((dim))
      for q in range(dim):
        s_q = np.random.random_sample()
        max_value = max(s_q, 1-s_q)
        mxArr[q] = s_q + 2*max_value
        mnArr[q] = s_q - 2*max_value
      return mxArr, mnArr

  #Builds each tree in the HST Forest
  def buildHST(self,mxArr, mnArr, k, h, dim):
    if k == h:
      return Node(k=k)
    node = Node()
    q = np.random.randint(dim)
    p = (mxArr[q] + mnArr[q])/2.0
    temp = mxArr[q]
    mxArr[q] = p
    node.lChild = self.buildHST(mxArr, mnArr, k+1, h, dim)
    mxArr[q] = temp
    mnArr[q] = p
    node.rChild = self.buildHST(mxArr, mnArr, k+1, h, dim)
    node.q = q
    node.p = p
    node.k = k
    return node

  #Updates Mass Profile of each node in the trees with incoming stream data
  '''
  x: Data stream
  node: Node in HST
  refWdwFlag: Specifies whether current x is in reference window or latest window
  '''
  def updateMassProfile(self,x, node, refWdwFlag):
    if(node):
      if(node.k != 0):
        if refWdwFlag:
          node.r += 1
        else:
          node.l += 1
      if(x[node.q] > node.p):
        node_new = node.rChild
      else:
        node_new = node.lChild
      self.updateMassProfile(x, node_new, refWdwFlag)

  #Collects score of x from each node in the tree
  def scoreTree(self,x,node, k):
    s = 0
    if(not node):
      return s
    s += node.r * (2**k)

    if(x[node.q] >node.p):
      node_new = node.rChild
    else:
      node_new = node.lChild
    s += self.scoreTree(x, node_new, k+1)
    return s

  #updates model on resetting the HST
  def updateResetModel(self,node):
    if(node):
      node.r = node.l
      node.l = 0
      self.updateResetModel(node.lChild)
      self.updateResetModel(node.rChild)

  #Prints each node content of the HST
  def printHST(self,node):
    if(node):
      print(('Dimension of the node is:%d and split value is:%f, k is:%d, reference_value:%d') %(node.q, node.p, node.k, node.r))
      self.printHST(node.lChild)
      self.printHST(node.rChild)
      
  #To fit to the current stream and build the tree and the list of scores from all trees in the forest
  def partialFit(self,X):
    dim = X.shape[1]
    scores = np.zeros((X.shape[0]))
    
    #Builds each tree and adds it to the forest
    for i in range(self.nt):
      mxArr, mnArr = self.genMaxMin(dim)
      tree = self.buildHST(mxArr, mnArr, 0, self.h, dim)
      self.HSTrees.append(tree)
      
    # Updates the mass profiles in the reference window from incoming stream
    for i in range(self.wdwSize):
      for tree in self.HSTrees:
        self.updateMassProfile(X[i], tree, True)
    count = 0
    for i in range(X.shape[0]):
      x = X[i]
      s = 0
      for tree in self.HSTrees:
        s = s + self.scoreTree(x, tree, 0)
        self.updateMassProfile(x, tree, False)
      print(('Score is %f for instance %d') %(s, i))
      scores[i] = s
      count += 1

      #If size of current window exceeds window size permissible, reset the tree
      if count == self.wdwSize:
        print('Reset tree')
        for tree in self.HSTrees:
          self.updateResetModel(tree)
        count = 0

    return scores
  
#For emulating data stream of size = size with anomalies after every 10th interval (sparse)
def genStream(size):
    dataStream = []

    for i in range(size):
        # Generate a random number
        data_point = random.uniform(0, 1)
        data_point2 = random.uniform(0, 1)
        data_point3 = random.uniform(0, 1)

        # Introduce anomaly after every 500th data point (sparse)
        if((i+1)%500==0):
            anomaly_value = random.uniform(5, 10)
            data_point += anomaly_value
            data_point2 += anomaly_value
            data_point3 += anomaly_value

        dataStream.append((data_point,data_point2,data_point3))

    return dataStream

if __name__ == '__main__':
    # Stream size
    streamSize = 5000

    X = genStream(streamSize)
    X = np.array(X)

    print(X.shape)
    hst_Forest = HST_Forest(250,25,15)
    finalScoreList= hst_Forest.partialFit(X)
    finalScoreList = np.array(finalScoreList)
    X_accepted = []

    #Threshold lower limit of each data stream item is calculated as: (online mean of final score list - std. deviation from current scores)
    threshold = np.mean(finalScoreList)-np.std(finalScoreList)
    for i,score in enumerate(finalScoreList):
        if(score>=threshold):
            X_accepted.append((X[i][0],X[i][1]))
        else:
            X_accepted.append((0,0))

    print("X: ",X)
    print("X_accepted:",X_accepted)
    print("finalScoreList:",finalScoreList)

    #Compare only one kind of data from each item
    X = X[:,0]
    X_accepted = np.array(X_accepted)
    X_accepted = X_accepted[:,0]

    #Visualize the data
    fig, ax = plt.subplots()
    ax.plot(X, label='Original Data Stream')
    ax.plot(X_accepted,label = 'Accepted Data without Anomalies',linestyle='dashed')
    ax.legend()
    ax.set_title('Dynamic Data Stream Plot with and without anomalies')

    plt.show()