/*                 Master Solution              */




/*                Individual Solutions              */

//     Bryan
function minimalTree(arr) {
  var tree = new BinaryNode();

  if(arr.length === 0) return null;
  if(arr.length === 1) {
    tree.val = arr[0];
    return tree;
  }
  var mid = Math.floor(arr.length / 2);
  var left = arr.slice(0,mid);
  var right = arr.slice(mid+1);
  tree.val = arr[mid];
  tree.leftChild = minimalTree(left);
  tree.rightChild = minimalTree(right);

  return tree;
}

function BinaryNode(value) {
  this.val = value;
  this.leftChild;
  this.rightChild;
}

BinaryNode.prototype.depth = function(maxDepth, curDepth) {
  maxDepth = maxDepth || 0;
  curDepth = curDepth || 0;

  if(this.leftChild === undefined && this.rightChild === undefined) {
    if(curDepth > maxDepth) return curDepth;
    return maxDepth;
  }
  maxDepth = this.leftChild ? this.leftChild.depth(maxDepth, curDepth+1) : maxDepth;
  maxDepth = this.rightChild ? this.rightChild.depth(maxDepth, curDepth+1) : maxDepth;

  return maxDepth;
};