import { decrement, increment, multiply, reducer, reset } from './counter'

test('should return the initial state', () => {
  expect(reducer(undefined, { type: undefined as any })).toMatchSnapshot()
})

test('should handle INCREMENT', () => {
  expect(reducer(undefined, increment())).toMatchSnapshot()
})

test('should handle DECREMENT', () => {
  expect(reducer(undefined, decrement())).toMatchSnapshot()
})

test('should handle MULTIPLY', () => {
  expect(reducer(undefined, multiply(2))).toMatchSnapshot()
})

test('should handle RESET', () => {
  expect(reducer(undefined, reset())).toMatchSnapshot()
})