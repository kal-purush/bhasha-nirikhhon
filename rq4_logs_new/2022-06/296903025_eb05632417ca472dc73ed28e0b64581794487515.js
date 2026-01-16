const { toNumber } = require('../dist/helpers')

describe('toNumber', () => {
  test('should return a number with given string', () => {
    expect(toNumber('1')).toBe(1)
  })

  test('should return a number with given number', () => {
    expect(toNumber(1)).toBe(1)
  })

  test('should return default value with given undefined', () => {
    expect(toNumber(undefined, 1)).toBe(1)
  })

  test('should return default value with given invalid number', () => {
    expect(toNumber('abc', 1)).toBe(1)
  })
})