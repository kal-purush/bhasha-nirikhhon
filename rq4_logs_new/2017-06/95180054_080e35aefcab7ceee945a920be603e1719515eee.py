import numpy as np
from numpy import *
import decimal
import functions as f
from testing import *
import numpy

# Source : kdtreeCOPY.py ( equivalent to scipy.spatial.kdtree )
# Use: Help build KD_Tree



class KD_Tree(object):
    """
    Binary Heap
    """
    def __init__(self, observer, resN, atoms, distLimit="", K=1):
        # Construct tree
        self.tree = self._construct(observer, resN, distLimit, atoms, K)
        self.children = 0

    class Node(object):
        def __init__(self, value, dist, LEFT=None, RIGHT=None):
            self.data = value
            self.left = LEFT
            self.right = RIGHT
            self.distance = dist


    def _insert(self, new, top):
        if top == None:
            self.children += 1
            return new
        elif top.left == None and top.right == None:
                if new.distance <= top.distance:
                    self.children += 1
                    top.left = new
                else:
                    self.children += 1
                    top.right = new
        else:
            if new.distance <= top.distance:
                return self._insert(new, top.left)
            else:
                return self._insert(new, top.right)
    # def _remove(self, top):
    #     # assume (for now) tree is balanced
    #     if self.children <= 1 or top.right == None:
    #         if top == None: # tree = 0
    #             raise Warning
    #         elif top.left == None and top.right == None: # tree = 1
    #             return None
    #         else: # tree has no right
    #             return top.left
    #     else:
    #         grandparent = top
    #         while grandparent.right.right != None:
    #             grandparent = grandparent.right
    #         grandparent.right = None
    #         return top


    def _update(self, new, K, top):
        # assume (for now) tree is balanced
        if self.children < K:
            raise Warning
        else:
            grandparent = top
            while grandparent.right.right != None:
                grandparent = grandparent.right
            if new.distance < grandparent.right.distance:
                grandparent.right = new
            return top

    def _construct(self, observer, Res, distLimit, atoms, K):
        self.Root = None
        for atom in atoms:
            if distLimit != "":
                if atom.resName == Res:

                    dist = f.euclideanDistance([observer.x, observer.y, observer.z], [atom.x, atom.y, atom.z])
                    if dist <= distLimit:
                        curr = self.Node(atom, observer)

                        if self.children < K:
                            self.Root = self._insert(curr, self.Root)
                        else:
                            self.Root = self._update(curr, K, self.Root)
            return self.Root


# kNN_resNames here to compare code structure
"""
def kNN_resNames(Atoms, K, distance, observer, resname):

    # initialize root
    neighbors = []
    walking = []

    # same
    for atom in Atoms:
            # same
            if atom.resName.strip() == resname:
                # same
                d = euclideanDistance((atom.x, atom.y, atom.z), (observer.x, observer.y, observer.z))
                # same
                if d <= distance:
                    # compare children to K
                    if len(neighbors) < K:
                        # _insert
                        neighbors.append(atom.name)
                        walking.append(d)
                    # compare children to K
                    elif len(neighbors) == K:
                        # _update:

                        # temp: top
                        max = d
                        marker = K+1
                        # while loop moving only to right of tree structure until temp's right is None
                        for PO in range(len(walking)):
                            if max < walking[PO]:
                                marker = PO
                                max = walking[PO]
                        if marker < K+1:
                            neighbors[marker] = atom.name
                            walking[marker] = d
    # return tree
    return neighbors
"""


def treeTesting():
    pdbID = raw_input("Enter a pdb code: ")
    path = urllib.urlretrieve('http://files.rcsb.org/download/%s.pdb' % pdbID,
                              'C:/Users/Brianna/Downloads/proteinDATA/%s.pdb' % pdbID)
    try:
        file = 'C:/Users/blm7643/Downloads/proteinDATA/%s.pdb' % pdbID
        pdbData = readFile(file)
    except IOError:
        file = '%s.pdb' % pdbID
        pdbData = readFile(file)

    Atoms = grabStandardAtomsFromPDB(pdbData)
    desiredAtoms = getSpecificAtom("CB", "GLU", Atoms)

    distance = 11.14
    constraint = ["SG", "CB", "CA", "C", "N", "O"]
    constraint.sort()
    K = len(constraint)



treeTesting()