import { beforeEach, describe, expect, it } from '@jest/globals';
import { List } from '../src/helpers/list';
import { setTimeout } from 'timers/promises';


function loadList(num: number, list: List<Number>, func: 'lpush' | 'rpush' = 'lpush') {
  for(let i = 0; i < num; i++) {
    list[func](i);
  } 
}

const testLists = [
  { count: 1, },
  { count: 3, },
  { count: 6 }
]

const asyncCases = [
  { delay: 0, timeout: undefined, expected: 0 },
  { delay: 2, timeout: undefined, expected: 0 },
  { delay: 2, timeout: 3, expected: 0 },
  { delay: 3, timeout: 2, expected: 0 }
]


describe('List Tests', () => {
  let list: List<Number> = new List();

  beforeEach(() => { 
    list = new List();
  })

  it('is created empty when no data provided', () => {
    expect(list.len).toBe(0);
    expect(list.head).toBe(null);
    expect(list.tail).toBe(null);
  })

  it('can be created with 1 item initially', () => {
    list = new List(0);
    expect(list.len).toBe(1);
  })

  it.each(testLists)('can lpush/lpop $count items', ({ count }) => {
    loadList(count, list, 'lpush');
    expect(list.len).toBe(count);
    for(let i = count - 1; i >= 0; i--) {
      expect(list.lpop()).toBe(i)
    }
    expect(list.len).toBe(0);
  })

  it.each(testLists)('can rpush/rpop $count items', ({ count }) => {
    loadList(count, list, 'rpush');
    expect(list.len).toBe(count);
    for(let i = count - 1; i >= 0; i--) {
      expect(list.rpop()).toBe(i)
    }
    expect(list.len).toBe(0);
  })

  it.each(testLists)('can lpush/rpop $count items', ({ count }) => {
    loadList(count, list, 'lpush');
    expect(list.len).toBe(count);
    for(let i = 0; i < count; i++) {
      expect(list.rpop()).toBe(i)
    }
    expect(list.len).toBe(0);
  })

  it.each(testLists)('can rpush/lpop $count items', ({ count }) => {
    loadList(count, list, 'rpush');
    expect(list.len).toBe(count);
    for(let i = 0; i < count; i++) {
      expect(list.lpop()).toBe(i)
    }
    expect(list.len).toBe(0);
  })

  it.each(asyncCases)('brpop: delay - $delay | timeout - $timeout', async ({ delay, timeout, expected }) => {
    let ac = new AbortController();
    let start = 0, end = 0;
    let actual = await new Promise((resolve) => {
      list.brpop(timeout)
        .then(val => {
          end = Date.now();
          ac.abort();
          resolve(val);
        })
      start = Date.now();
      setTimeout(delay * 1000, [null], { signal: ac.signal })
        .then(() => list.rpush(expected))
        .catch(err => { if(err.code !== 'ABORT_ERR') throw err })
    })

    let elapsed = Math.floor((end - start) / 1000);
    if(!timeout || timeout > delay) {
      expect(actual).toBe(expected);
      expect(elapsed).toBe(delay);
    } else {
      expect(actual).toBe(null);
      expect(elapsed).toBe(timeout);
    }
  }, 10000)

  it('cancels brpop with a timer by calling list.abortBlocks', async () => {
    let start = 0, end = 0;
    let timeout = 10;
    let actual = await new Promise((resolve) => {
      start = Date.now();
      list.brpop(timeout)
        .then(val => {
          end = Date.now();
          resolve(val);
        })
      list.abortBlocks();
    })

    let elapsed = Math.floor((end - start) / 1000);
    expect(actual).toBe(null);
    expect(elapsed).toBe(0);
  })

  it('cancels brpop without a timer by calling list.abortBlocks', async () => {
    let start = 0, end = 0;
    let actual = await new Promise((resolve) => {
      start = Date.now();
      list.brpop()
        .then(val => {
          end = Date.now();
          resolve(val);
        })
      list.abortBlocks();
    })

    let elapsed = Math.floor((end - start) / 1000);
    expect(actual).toBe(null);
    expect(elapsed).toBe(0);
  })
})