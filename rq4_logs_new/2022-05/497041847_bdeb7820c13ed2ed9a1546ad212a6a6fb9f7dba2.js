class LLNode {
  constructor(data, next = null) {
    this.value = data;
    this.next = next;
  }
}

export class SinglyLinkedList {
  constructor() {
    this.head = null;
  }
  push(data) {
    let newNode = new LLNode(data);
    // If the list is empty, then the first node is the head.
    if(!this.head) {
      this.head = newNode;
      return this.head;
    }
    let current = this.head;
    while(current.next !== null) {
      current = current.next;
    }
    current.next = newNode;
    return this.head
  }
}