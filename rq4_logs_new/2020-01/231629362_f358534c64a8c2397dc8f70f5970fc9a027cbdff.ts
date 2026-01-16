import { Node } from './node';

export class BinarySearchTree {
  public root: Node;
  constructor(rootValue: any) {
    this.root = new Node(rootValue);
  }

  public insert(currentNode: any, newValue: any) {
    if (currentNode === null) {
      currentNode = new Node(newValue);
    } else if (newValue < currentNode.val) {
      currentNode.leftChild = this.insert(currentNode.leftChild, newValue);
    } else {
      currentNode.rightChild = this.insert(currentNode.rightChild, newValue);
    }
    return currentNode;
  }

  public insertBST(newValue: any) {
    if (this.root == null) {
      this.root = new Node(newValue);
      return;
    }
    this.insert(this.root, newValue);
  }

  public preOrderPrint(currentNode: Node) {
    if (currentNode !== null) {
      console.log(currentNode.value);
      this.preOrderPrint(currentNode.leftChild);
      this.preOrderPrint(currentNode.rightChild);
    }
  }

  public inOrderPrint(currentNode: Node) {
    if (currentNode !== null) {
      this.inOrderPrint(currentNode.leftChild);
      console.log(currentNode.value);
      this.inOrderPrint(currentNode.rightChild);
    }
  }

  public postOrderPrint(currentNode: Node) {
    if (currentNode !== null) {
      this.postOrderPrint(currentNode.leftChild);
      this.postOrderPrint(currentNode.rightChild);
      console.log(currentNode.value);
    }
  }

  public search(value): Node {
    let currentNode: Node = this.root;
    while (currentNode && currentNode.value !== value) {
      if (value < currentNode.value) {
        currentNode = currentNode.leftChild;
      } else {
        currentNode = currentNode.rightChild;
      }
    }
    return currentNode;
  }

  public delete(currentNode: Node, value: any) {
    if (currentNode === null) {
      return false;
    }

    let parentNode: Node;
    while (currentNode && currentNode.value !== value) {
      parentNode = currentNode;
      if (value < currentNode.value) {
        currentNode = currentNode.leftChild;
      } else {
        currentNode = currentNode.rightChild;
      }
    }

    if (currentNode === null) {
      return false;
    } else if (
      currentNode.leftChild == null &&
      currentNode.rightChild == null
    ) {
      if (currentNode.value === this.root.value) {
        this.root = null;
        return true;
      } else if (currentNode.value < parentNode.value) {
        parentNode.leftChild = null;
        return true;
      } else {
        parentNode.rightChild = null;
        return true;
      }
    } else if (currentNode.rightChild == null) {
      if (currentNode.value === this.root.value) {
        this.root = currentNode.leftChild;
        return true;
      } else if (currentNode.leftChild.val < parentNode.value) {
        parentNode.leftChild = currentNode.leftChild;
        return true;
      } else {
        parentNode.rightChild = currentNode.leftChild;
        return true;
      }
    } else if (currentNode.leftChild == null) {
      if (currentNode.value === this.root.value) {
        this.root = currentNode.rightChild;
        return true;
      } else if (currentNode.rightChild.val < parentNode.value) {
        parentNode.leftChild = currentNode.rightChild;
        return true;
      } else {
        parentNode.rightChild = currentNode.rightChild;
        return true;
      }
    } else {
      let minRight: Node = currentNode.rightChild;
      while (minRight.leftChild !== null) {
        minRight = minRight.leftChild;
      }
      const temp = minRight.value;
      this.delete(this.root, minRight.value);
      currentNode.value = temp;
      return true;
    }
  }

  public findMin(rootNode: Node) {
    if (rootNode === null) {
      return null;
    } else if (rootNode.leftChild === null) {
      return rootNode.value;
    } else {
      return this.findMin(rootNode.leftChild);
    }
  }
}