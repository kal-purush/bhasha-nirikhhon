type NodeType<T> = {
  value: T;
  next: NodeType<T>|null;
};

const createNode = <T>(element: T): NodeType<T> => {
  let next: NodeType<T>|null = null;
  return {
    value: element,
    next
  }
};

type ListType<T> = {
  head: NodeType<T>|null;
  size: number;
  add: (value: T) => void;
  findLast: () => NodeType<T>|null
};

const createList = <T>(): ListType<T> => {
  let size = 0;
  let head: NodeType<T>|null = null;

  return {
    head,
    size,
    find(element) {},
    findLast() {
      let tmp = head;
      while (tmp && tmp.next) {
        tmp = tmp.next;
      }
      return tmp;
    },
    add(value: T) {
      const node: NodeType<T> = createNode(value);

      if (!head) {
        head = node;
      } else {
        const lastNode = this.findLast();
        if (lastNode) {
          lastNode.next = node;
        }
      }

      size += 1;
    },
  };
}