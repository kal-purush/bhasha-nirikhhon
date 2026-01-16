import {
  LinkedList,
  LinkedListNode,
  createLinkedList
} from '../List';

export type HashTable<T> = {
  add: (value: T) => string
  delete: (value: T) => T|null
  get: (key: string) => Array<T>
  keys: () => Array<string>,
  values: () => Array<T>
}


export const createHashTable = <T>(hashFunction: (v: T) => string): HashTable<T> => {
  const storage: { [key: string]: LinkedList<T> } = Object.create(null);

  return {
    add(value: T) {
      const key: string = hashFunction(value);
      if (storage[key]) {
        storage[key].append(value);
      } else {
        storage[key] = createLinkedList<T>();
        storage[key].append(value);
      }
      return key;
    },

    delete(value: T) {
      const hashKey = hashFunction(value);
      if (storage[hashKey]) {
        const deletedNode: LinkedListNode<T>|null = storage[hashKey].remove(value);
        return deletedNode ? deletedNode.value : null;
      }
      return null;
    },

    get(key: string) {
      const targetBucket = storage[key];
      return targetBucket ? targetBucket.toArray() : [];
    },

    keys() {
      return Object.keys(storage);
    },

    values() {
      return Object.values(storage).reduce(
        (acc, item: LinkedList<any>) => acc.concat(...item.toArray()),
        []
      );
    }
  };
};

export default createHashTable;