import {Node, LinkedList} from './linked_lists.js'

class Stack {
    constructor() {
        this.items = new LinkedList();
    }

    //getting what's at the top of the stack, without removing it
    peek() {
        if (!this.items.head) {
            return null;
        }
        return this.items.head.value;
    }

    //getting what's at the top of the stack and removing it
    pop() {
        if (!this.items.head) {
            return null;
        }
        let oldHead = this.items.head;
        this.items.head = oldHead.next;
        oldHead.next = null;
        return oldHead.value;
    }

    //adding something new to the top of the stack
    push(value) {
        const nuNode = new Node(value);
        if (!this.items.head) {
            this.items.head = nuNode;
            return this;
        }
        nuNode.next = this.items.head;
        this.items.head = nuNode;
        return this;
    }
}

const stack = new Stack();
stack.push(1)
stack.push(4)
stack.push(2)
stack.push(13)
console.log('Top of stack is: ', stack.peek())
const poppedItem = stack.pop()
console.log('We popped off the stack: ', poppedItem)
console.log('Now the top of the stack is: ', stack.peek())

// LinkedList
// what is the top?  head or the tail?  Top = head
// stack.peek() - this.items.head - Big(0) = O(1)
// stack.pop() - return the head, and make the second item the new head - Big(0) = O(1)
// stack.push(object) - put something at the head - Big(0) = O(1)

// Array
// what is the top?
// [0, 3, 2, 1, 5, 6] - at the end
// [0, 3, 2, 1, 5] - at the beginning

// const fun = new Array(6)
// fun.push(1) // 0
// fun.push(4) // 1
// fun.push(5)
// fun.push(1)
// fun.push(4)
// fun.push(6) // 5
// fun.push(11) // create a whole new array of size 12, and then copy it over.  Big O(n)
//   ArrayList.size = 6.  
//    [ 1, 4, 5, 1, 4, 6]
//   new Array(this.size * 2)
//   copy everything over
// fun.push(8) // O(1)

// stack.peek() - at end O(1), at beginnig O(1)
// stack.pop() - at end O(1), at beginnig O(n)
// stack.push(object) - at end amortized 0(1), at beginning O(n)


// Stack LIFO - last in first out 
// Queue FIFO - first in first out 