// Prompt: implement a function to delete a node in the middle. 
// Not exact middle, but between first and last node.

const deleteMiddleNode = (node, list) => {
  if (!node || !list) {
    return new Error ('invalid inputs');
  }
  
  while (list.next) {
    if (list.next === node) {
      list.next = list.next.next;
    }
    list = list.next;
  }
};