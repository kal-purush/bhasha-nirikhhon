import { Queue } from '.'

describe('Queue', () => {
  it('should remove an item from the queue', () => {
    const queue = new Queue<number>()

    queue.add(1)
    queue.add(2)
    queue.add(3)

    expect(queue.remove()).toBe(1)

    expect(queue.remove()).toBe(2)

    expect(queue.remove()).toBe(3)
  })

  it('should search for an item', () => {
    const queue = new Queue<number>()

    queue.add(1)
    queue.add(2)
    queue.add(3)

    expect(queue.search(2)).toBe(1)
  })

  it('should return -1 if the item does not exists', () => {
    const queue = new Queue<number>()

    queue.add(1)
    queue.add(2)
    queue.add(3)

    expect(queue.search(5)).toBe(-1)
  })
})