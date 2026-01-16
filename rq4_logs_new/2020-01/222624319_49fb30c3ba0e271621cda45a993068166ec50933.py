indexleft = 0
idnexright = 0
class Node :

    def __init__(self, data):
        
        self.left = None
        self.right = None
        self.data = data

    def insert(self,data) :

        if self.data :
            if data < self.data :
                if self.left is None :
                    self.left = Node(data)
                else :
                    self.left.insert(data)
            elif data > self.data :
                if self.right is None :
                    self.right = Node(data)
                else :
                    self.right.insert(data)
        else :
            self.data = data
    
    def Deletion(self, dele) :
        global index

        if dele == self.data :
            indexleft = 0
            indexright = 0
            if self.left is None :
                indexleft = 1
            if self.right is None : 
                indexright = 1
            if indexleft == 1 and indexright == 1 :
                self.data = ""
            else :
                indexleft = 0
                indexright = 0
                if self.left is not None :
                    indexleft = 1
                if self.right is not None : 
                    indexright = 1
                if indexleft == 1 and indexright == 1 :
                    self.data = self.left.data
                    self.left.data = ""
                else :
                    if indexleft == 1 and indexright != 1 :
                        self.data = self.left.data
                        self.left.data = ""
                    if indexright == 1 and indexleft != 1 :
                        self.data =  self.right.data
                        self.right.data = ""
        elif dele != self.data :
            if dele < self.data :
                if self.left is None :
                    pass
                return self.left.Deletion(dele)
            elif dele > self.data :
                if self.right is None :
                    pass
                return self.right.Deletion(dele)

    def PrintTree(self) :
        if self.left :
            self.left.PrintTree()
        print(self.data),
        if self.right :
            self.right.PrintTree()
    
    def inorderTraversal(self, root) :
        res = []
        if root :
            res = self.inorderTraversal(root.left)
            res.append(root.data)
            res = res + self.inorderTraversal(root.right)
        return res

root = Node(100)
root.insert(50)
root.insert(30)
root.insert(60)
root.insert(150)
root.insert(120)


print(root.inorderTraversal(root))
root.Deletion(120)

print(root.inorderTraversal(root))