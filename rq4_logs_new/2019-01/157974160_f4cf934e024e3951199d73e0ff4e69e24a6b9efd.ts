import {
  LinkedList,
  LinkedListNode,
  createLinkedListNode,
  createLinkedList,
} from './List';

describe('LinkedList', () => {
  test('LinkedListNode', () => {
    const v = 'A';
    const itemNode: LinkedListNode<string> = createLinkedListNode(v);

    expect(itemNode.value).toBe(v);
  });

  test('LinkedList', () => {
    const lettersList: LinkedList<string> = createLinkedList();

    expect(lettersList.head).toBe(null);
    expect(lettersList.isEmpty()).toBe(true);

    lettersList.append('A');
    lettersList.append('B');

    console.log(lettersList.last());
    expect(lettersList.isEmpty()).toBe(false);
    expect(lettersList.length()).toBe(2);
  });
});