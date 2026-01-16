from __future__ import print_function  # in case you are running python 2
from BST import BSTNode,BST
import random

class SplayTree(BST):
    """A splay tree.
    """

    global left_rotate, right_rotate

    def left_rotate(self, x):
        y = x.right
        x.right = y.left
        if y.left != self.nil:
            y.left.p = x
        y.p = x.p
        if x.p == self.nil:
            self.root = y
        elif x ==x.p.left:
            x.p.left = y
        else:
            x.p.right = y
        y.left = x
        x.p = y

    def right_rotate(self, x):
        y = x.left
        x.left = y.right
        if y.right != self.nil:
            y.right.p = x
        y.p = x.p 
        if x.p == self.nil:
            self.root = y
        elif x == x.p.right:
            x.p.right = y
        else:
            x.p.left = y
        y.right = x
        x.p = y

    def splay(self, z):
        while z.p != self.nil:
            if z.p.p == self.nil:
                ##zig Right
                if z == z.p.left:
                    right_rotate(self, z.p)
                ##zig left
                else:
                    left_rotate(self, z.p)
            elif ((z.p.left == z) and (z.p == z.p.p.right)):
                right_rotate(self, z.p)
                left_rotate(self, z.p)
            elif ((z.p.left == z) and (z.p == z.p.p.left)):
                right_rotate(self, z.p.p)
                right_rotate(self, z.p)
            elif ((z.p.right == z) and (z.p == z.p.p.right)):
                left_rotate(self, z.p.p)
                left_rotate(self, z.p)
            else:
                left_rotate(self, z.p)
                right_rotate(self, z.p)
                
                    
    def search(self, k):
        x = self.root
        while k != x.key and x != self.nil:
            if k < x.key:
                par = x
                x = x.left
            else:
                par = x
                x = x.right
        if x != self.nil:
            self.splay(x)
        else:
            self.splay(par)
        return x
    
    def insert(self, z):
        BST.insert(self, z)
        self.splay(z)

    def delete(self, z):
        if z.left == self.nil:
            par = z.p
            self.transplant(z, z.right)
        elif z.right == self.nil:
            par = z.p
            self.transplant(z, z.left)
        else:
            y = z.right.minimum(self.nil)
            if y.p != z:
                self.transplant(y, y.right)
                y.right = z.right
                y.right.p = y
            self.transplant(z, y)
            y.left = z.left
            y.left.p = y
            par = y.p
        self.splay(par)

def main():
    splayTree = SplayTree()
    print("First insert 20 random nodes into an empty tree and print the tree.")
    for i in range(20):
        n = random.randint(0, 1000)
        splayTree.insert(BSTNode(n, splayTree.nil, splayTree.nil, splayTree.nil))
    print()
    splayTree.print()
    print()

    print("\nThen insert 80 more random nodes into the tree.")
    for i in range(80):
        n = random.randint(0, 1000)
        splayTree.insert(BSTNode(n, splayTree.nil, splayTree.nil, splayTree.nil))
    print("These keys in inorder are: ")
    splayTree.inorder()
    print()

    print("\nNow do 200 searches. ")
    for i in range(200):
        n = random.randint(0, 1000)
        if splayTree.search(n) == splayTree.nil:
            print(n, " not found")
        else:
            print(n, " found")

    print("\nNow search for & delete some nodes.")
    for i in range(2000):
        n = random.randint(0, 1000)
        z = splayTree.search(n)
        if z != splayTree.nil:
            splayTree.delete(z)
    print("The keys in the trees in inorder are now: ")
    splayTree.inorder()
    print()

    print("\nThe final tree looks like this.")
    print()
    splayTree.print()
    print()

if __name__ == "__main__":
    main()