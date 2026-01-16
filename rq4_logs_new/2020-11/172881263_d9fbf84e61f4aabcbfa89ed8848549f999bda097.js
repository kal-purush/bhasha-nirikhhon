class DoublyLinkedList {
    constructor(value) {
        this.head = new Node(value);
        this.tail = this.head;
        this.length = 1;
    }

    append(value) {
        const newNode = new Node(value);
        newNode.prev = this.tail;
        this.tail.next = newNode;
        this.tail = newNode;
        this.length++;
        return this;
    }

    prepend(value) {
        const newNode = new Node(value);
        this.head.prev = newNode;
        newNode.next = this.head;
        this.head = newNode;
        this.length++;
        return this;
    }

    printList() {
        const array = [];
        let currentNode = this.head;
        while (currentNode != null) {
            array.push(currentNode.value);
            currentNode = currentNode.next;
        }
        return array;
    }

    insert(index, value) {
        if (index >= this.length) {
            return this.append(value);
        }

        if (index === 0) {
            this.prepend(value);
            return this;
        }
        const newNode = new Node(value);
        const previousNode = this.traverse(index-1);
        const nextNode = previousNode.next;

        previousNode.next = newNode;
        newNode.prev = previousNode;
        newNode.next = nextNode;
        nextNode.prev = newNode;
        this.length++;
        return this;
    }

    remove(index) {
        const previousNode = this.traverse(index-1);
        const unwantedNode = previousNode.next;
        previousNode.next = unwantedNode.next;
        this.length--;
        return this;
    }

    traverse(index) {
        let currentNode = this.head;
        for (let i = 0; i < index; i++) {
            currentNode = currentNode.next;
        }
        return currentNode;
    }
}

class Node {
    constructor(value) {
        this.value = value;
        this.next = null;
        this.prev = null;
    }
}

const lList = new DoublyLinkedList(10);
lList.append(5);
lList.append(16);
lList.prepend(4);
// lList.insert(2, 99);
// lList.remove(1);
console.log(lList);
console.log(lList.printList());