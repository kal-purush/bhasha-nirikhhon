const nodes = {
  node: 10,
  left: {
    node: 20,
    left: {
      node: 5,
    },
  },
  right: {
    node: 30,
    left: {
      node: 15,
      right: {
        node: 5
      }
    }
  }
}

const node = 5;

function findNode(nodeses, node) {
  const objectArr = [];
  objectArr.push(nodeses)
  const current = objectArr.pop()
  
  while (objectArr.length > 0) {
    if (current.node === node) {
      return current;
    }
    if (current.left) {
      objectArr.push(current.left)
    }
    if (current.right) {
      objectArr.push(current.right)
    }
  }
}

console.log(findNode(nodes, node))