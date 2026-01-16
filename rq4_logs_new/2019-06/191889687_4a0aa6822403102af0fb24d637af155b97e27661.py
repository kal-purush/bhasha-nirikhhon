# coding: utf-8
# Your code here!
from collections import deque
class Node:
    def __init__(self,data):
        self.data=data
        self.left=None
        self.right=None
        self.hd=None
def findBottomView(root):
    if root==None:
        return
    hd=0
    root.hd=hd
    hd_dict={}
    q=deque()
    q.append(root)
    while q:
        temp=q.popleft()
        hd=temp.hd
        hd_dict[hd]=hd_dict.get(hd,0)
        hd_dict[hd]=temp.data
        if temp.left!=None:
            q.append(temp.left)
            temp.left.hd=temp.hd-1
        if temp.right!=None:
            q.append(temp.right)
            temp.right.hd=temp.hd+1
    result=hd_dict.values()
    print ' '.join(str(x) for x in result)
root=Node(20)
root.left=Node(8)
root.right=Node(22)
root.left.left=Node(5)
root.left.right=Node(30)
root.right.left=Node(4)
root.right.right=Node(25)
root.left.right.left=Node(10)
root.left.right.right=Node(14)

findBottomView(root)