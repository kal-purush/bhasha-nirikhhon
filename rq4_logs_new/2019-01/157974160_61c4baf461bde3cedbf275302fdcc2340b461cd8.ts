import {
  LinkedListNode,
  LinkedList,
  createLinkedList,
} from '../List';

export type Stack<T> = {
  push: (value: T) => void
  pop: () => T|null
  isEmpty: () => boolean
}

export const createStack = <T>(): Stack<T> => {
  const store: LinkedList<T> = createLinkedList<T>();

  return {
    push(value: T) {
      store.append(value);
    },

    pop() {
      const firstNode: LinkedListNode<T>|null = store.first();
      if (firstNode) {
        store.head = firstNode.next;
      }

      return firstNode ? firstNode.value : null
    },

    isEmpty() {
      return store.isEmpty();
    }
  };
};

export default createStack;