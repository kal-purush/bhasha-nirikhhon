'use strict';

/**
 * @return {[type]} [description]
 */
var BinarySearchTree = module.exports = function() {
  this.root = null;
};

/**
 * Add a new node into the binary search tree.
 *
 * @param {[type]} value The value to be inserted
 * @return {Boolean} true if operation was successful
 */
BinarySearchTree.prototype.add = function(value) {
  var newNode = new Node(value);

  if (!this.root) {
    this.root = newNode;
    return true;
  }

  var node = this.root;
  while (node) {
    if (value < node.value) {
      // If there is nod left, then the new node belongs there
      if (node.left === null) {
        node.left = newNode;
        return true;
      } else {
        node = node.left;
      }
    } else if (value > node.value) {
      // If there is no right, then the new node belongs there
      if (node.right === null) {
        node.right = newNode;
        return true;
      } else {
        node = node.right;
      }
    } else {
      // If the new value is equal to the current one, just ignore.
      return false;
    }
  }
};

/**
 * Determine whether or not the value is present in the tree.
 *
 * @param  {Number} value The value to see if it is present in the tree.
 * @return {Boolean} true if the value is present in the tree.
 */
BinarySearchTree.prototype.contains = function(value) {
  var node = this.root;
  while (node) {
    if (value < node.value) {
      node = node.left;
    } else if (value > node.value) {
      node = node.right;
    } else {
      return true;
    }
  }
  return false;
};

/**
 * Traverse the tree recursivvely.
 *
 * @param  {Function} fn The function that is run on each node in the tree
 */
BinarySearchTree.prototype.traverse = function(fn) {
  // Helper function that is performed on each node by processing the left
  // subtree, then the node itself, then the right subtree.
  var inOrder = function(node) {
    if (node) {
      // Traverse the left subtree.
      if (node.left !== null) {
        inOrder(node.left);
      }

      // Call the passed function on this node.
      fn.call(this, node);

      if (node.right !== null) {
        inOrder(node.right);
      }
    }
  };

  // Start with the root node.
  inOrder(this.root);
};


/**
 * Get the number of nodes exist in the tree.
 *
 * @return {Number} the number of nodes exist in the tree.
 */
BinarySearchTree.prototype.size = function() {
  var size = 0;
  this.traverse(function(node) {
    size++;
  });
  return size;
};

/**
 * Convert the tree to array.
 *
 * @return {Array} the array of nodes in the tree.
 */
BinarySearchTree.prototype.toArray = function() {
  var array = [];
  this.traverse(function(node) {
    array.push(node.value);
  });
  return array;
};

var Node = function(value) {
  this.value = value;
  this.left = null;
  this.right = null;
};