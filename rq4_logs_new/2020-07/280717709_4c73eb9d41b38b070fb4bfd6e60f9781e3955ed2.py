#Function for trimming binary tree
def trim(tree, minval, maxval):
    if not tree:
        return 0

    tree.leftChild = trim(tree.leftChild.key, minval, maxval)
    tree.rightChild = trim(tree.rightChild.key, minval, maxval)

    if(minval <= tree.key <= maxval):
        return tree

    if(tree.key < minval):
        return tree.rightChild

    if(tree.key > maxval):
        return tree.leftChild