import { Node, Key } from '../Node';

export class Lru {
  values = new Map<Key, Node>();

  public headKey!: Key;
  public tailKey!: Key;

  constructor(
    private limit = 10,
  ) {}

  get head(): any {
    return this.get(this.headKey);
  }

  get tail(): any {
    return this.get(this.tailKey);
  }

  get size(): number {
    return this.values.size;
  }

  has(key: Key): boolean {
    return this.values.has(key);
  }

  set(key: Key, value: any): this {
    if (this.has(key)) {
      const item = this.values.get(key) as Node;
      item.value = value;
      if (this.headKey !== key) {
        const oldHead = this.values.get(this.headKey);

        if (oldHead) {
          oldHead.next = key;
        }

        this.updateItemNextPrev(item);

        item.next = undefined;
        item.prev = this.headKey;

        this.headKey = key;
      }
    } else {
      if (this.limit > 0 && this.limit === this.size) {
        this.removeLast();
      }
      const oldHead = this.values.get(this.headKey);
      if (oldHead) {
        oldHead.next = key;
      }
      const item = new Node(key, value, undefined, this.headKey);
      this.headKey = key;
      this.values.set(key, item);
      if (this.size === 1) {
        this.tailKey = key;
      }
    }
    return this;
  }

  get(key: Key): any {
    if (this.has(key)) {
      const item = this.values.get(key) as Node;
      this.set(key, item.value);

      return item.value;
    }
    return undefined;
  }

  delete(key: Key): this {
    if (this.has(key)) {
      const item = this.values.get(key) as Node;
      this.values.delete(key);

      this.updateItemNextPrev(item);

      if (this.headKey === key) {
        this.headKey = item.prev;
      }
    }
    return this;
  }

  private updateItemNextPrev(item: Node) {
    const oldNext = this.values.get(item.next);
    const oldPrev = this.values.get(item.prev);
    if (this.tailKey === item.key) {
      this.tailKey = item.next;
    }
    if (oldNext) {
      oldNext.prev = item.prev;
    }
    if (oldPrev) {
      oldPrev.next = item.next;
    }
  }

  private removeLast(): this {
    const item = this.values.get(this.tailKey) as Node;
    this.values.delete(this.tailKey);

    const oldNext = this.values.get(item.next);

    if (oldNext) {
      this.tailKey = item.next;
      oldNext.prev = undefined;
    }
    return this;
  }
}