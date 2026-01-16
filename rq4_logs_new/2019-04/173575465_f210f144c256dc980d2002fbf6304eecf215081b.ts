import { observable, computed, effect } from './observable'

test('observable', () => {
  const x = observable(() => 1)
  expect(x.value).toBe(1)
})

test('computed', () => {
  const items = observable(() => ['apple', 'banana', 'cherry'])
  const reversedItems = computed(() => items.value.slice().reverse())
  items.value.push('d')
  expect(reversedItems.value).toEqual(['d', 'cherry', 'banana', 'apple'])
})

test('effect', () => {
  const values = observable(() => [1, 2, 3])
  const callback = jest.fn()
  effect(() => {
    callback(values.value)
  })
  values.value.push(4)
  expect(callback).toHaveBeenCalledTimes(2)
})