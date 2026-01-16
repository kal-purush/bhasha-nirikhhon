export class Stack<T> {
    #items: T[]

    constructor() {
      this.#items = []
    }
    
    /**
     * Pushes an item onto the top of this stack.
     * @param item The item that will be added to the top of the stack
     */
    push(item: T): void {
        this.#items.push(item)
    }

    empty(): boolean {
        return this.#items.length < 1
    }
     /**
     * Remove the item from the top of this stack.
     * @returns the top item
     */
    pop(): T {
       const topItem = this.peek()
       this.#items.pop()
       return topItem
    }

    peek(): T {
        if(this.empty()) {
            throw new Error('The stack is empty')
        }
        return this.#items[this.#items.length-1]
    }

    search(item: T): number {
       return this.#items.findIndex(currentItem => currentItem === item)
    }
}
