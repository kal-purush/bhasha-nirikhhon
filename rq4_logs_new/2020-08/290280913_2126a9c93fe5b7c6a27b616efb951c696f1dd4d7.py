#!/usr/bin/env python
# coding: utf-8

# In[3]:


from dataclasses import dataclass


# In[157]:


@dataclass
class node:
    left   = None
    right  = None
    key    : None
    height = 1
    
class TreeNode():
    def __init__(self, key):
        self.key = key
        self.left = None
        self.right = None
        self.height = 1
    
class AVL:
    
    
    def get_height(self, node):
        if not node:
            return 0
        else:
            return node.height
    
    def balancing_factor(self, node):
        if not node:
            return 0
        else:
            return self.get_height(node.left) - self.get_height(node.right)
        
    def rightRotate(self, root):
        
        """
        Since this RR rotate, we have imbalance in currRoot :  curRoot -- > left -- child_left
        Now to do RR, we need move 'left' to curRoot and curRoot to left.right and left.right to curRoot.left
        """
        print("inside right rotate")
        new_root = root.left
        #r_t = left/'new node'( going to be root node)'s right child
        r_t = new_root.right
        # now swap, make left as root by moveing root to left.right ( this is why we store r_t so that we dont lose left.right actual value)
        new_root.right = root
        #move left.right's older value ( before swap) to the swapped curRoot nodes right left child which is now free ( earlier it was 'new node')
        root.left = r_t
        root.height = 1+max(self.get_height(root.left), self.get_height(root.right))
        new_root.height = 1+max(self.get_height(new_root.left), self.get_height(new_root.right))
        
        print("new_root key = ", new_root.key)
        print("new_root left = ", new_root.left)
        print("new_root right = ", new_root.right)
        
        return new_root
        
        
    def leftRotate(self, root):
        """
        Since this LL rotate, we have imbalance in currRoot :  curRoot -- > right -- right's_Right_Child
        Now to do LL, we need move 'right' to curRoot and curRoot to right's.right and right's.right to curRoot.left
        """
        print("inside left rotate")
        new_root = root.right
        #r_t = left/'new node'( going to be root node)'s left child
        l_t = new_root.left
        # now swap, make rihgt as root by moveing root to right's.left ( this is why we store l_t so that we dont lose right.left's actual value)
        new_root.left = root
        #move left.right's older value ( before swap) to the swapped curRoot nodes right left child which is now free ( earlier it was 'new node')
        root.right = l_t
        
        root.height = 1+max(self.get_height(root.left), self.get_height(root.right))
        new_root.height = 1+max(self.get_height(new_root.left), self.get_height(new_root.right))

        print("new_root key = ", new_root.key)
        print("new_root left = ", new_root.left.key)
        print("new_root right = ", new_root.right.key)
        
        return new_root
    
    def insert(self, root_node, node):
        
        """
        insert a node to the binary tree
        arguments:
        root_node - top most node to travese the tree and find the position to insert the node
        node - node to be inserted 
        
        """
        if root_node is None:
            return node
            
        else:
            #check if node is present in left subtree or right subtree
            if node.key <= root_node.key:
                root_node.left = self.insert(root_node.left, node)
                
            else:
                root_node.right = self.insert(root_node.right, node)
                    
        root_node.height = 1 + max(self.get_height(root_node.left), self.get_height(root_node.right))
        
        #get the balancing factor of the root node and check if it >1 or <1. if true, the tree is imbalnced with the current
        #root node and balance and move up the treebalancing upper root nodes if they are imbalanced
        bf = self.balancing_factor(root_node)
        
        if bf>1:
            #imbalance due to either LL or LR
            if node.key < root_node.left.key:
                """
                if bf of root.left >=0 then it means that root's left child has a left subtree or a left leaf node because of 
                which root's left child's height ! =0  (h!=0) and hence we get subtree/tree with curRoot --> left --> left(inserted)
                LL imbalance
                """
                
                Print("performing right rotate")
                self.rightRotate(root_node)
            else:
                """
                if bf of root_node.left<=0 then it means root's left child has no left subtree or left leaf. Therefore tree is
                curRoot --> left --> right, Therefore this is left right imbllance
                """
                print("performing left right rorate")
                root_node.left = self.leftRotate(root_node.left)
                return self.rightRotate(root_node)
            
        if bf<-1:
            if node.key > root_node.right.key:
                print("performing left rotate rr")
                """
                if bf of root.right >=0 then it means that root's right child has a left subtree or a right leaf node because of 
                which root's right child's height ! =0  (h!=0) and hence we get subtree/tree with curRoot --> right --> right(inserted)
                RR imbalance
                """
                return self.leftRotate(root_node)
            else:
                print("performing right left rotatae rr")
                """
                if bf of root_node.right<=0 then it means root's right child has no left subtree or right leaf. Therefore tree is
                curRoot --> right --> left, Therefore this is left right imbalance
                """
                root_node.right = self.rightRotate(root_node.right)
                return self.leftRotate(root_node)
            
        return root_node
            
            
    def inorder_travs(self,start_node):
        #if the node exists, then left, root, right
        if start_node: 
            
            self.inorder_travs(start_node.left) 
            
            print(start_node.key) 
            
            self.inorder_travs(start_node.right)

    def Postorder_travs(self, root): 
        #if the node exists then left, right, root
        if root: 
            self.Postorder_travs(root.left) 
  
            self.Postorder_travs(root.right) 
   
            print(root.key)

    def preorder_travs(self, root):
        #if the node exists then left, right, root
        if root:
            
            print(root.key)
            
            self.preorder_travs(root.left)
            
            self.preorder_travs(root.right)
                
        
                    
        
    def delete(root, node):
        pass


# In[158]:


A = [33, 13, 52, 9, 21, 61, 8, 11]
B = [10,20,30,40,50,25]
root = None
Tree = AVL()
#for i in range(0, len(B)):
    #print("inserting --", B[i])
#    root = Tree.insert(root, TreeNode(B[i]))
print("inserting -- ", 10) 
#print("root = ", root)
root = Tree.insert(root, TreeNode(10))
#print("root = ", root.key)
print("inserting -- ", 20)
root = Tree.insert(root, TreeNode(20))
print("inserting -- ", 30)
#print("root = ", root.key)
root = Tree.insert(root, TreeNode(30))
#print("root = ", root.key)
root = Tree.insert(root, TreeNode(40))
#print("root = ", root.key)
root = Tree.insert(root, TreeNode(50))
#print("root = ", root.key)
root = Tree.insert(root, TreeNode(25))
#print("root = ", root.key)
    
Tree.preorder_travs(root)

#print(root.key)
#print(root.left.key)
#print(root.right.key)
#print((root.right).right.key)


# In[ ]:




 


# In[ ]:



