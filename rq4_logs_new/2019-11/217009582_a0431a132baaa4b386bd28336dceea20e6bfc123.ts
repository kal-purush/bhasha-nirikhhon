import { createFormatter } from '../../src'

describe('format dec option', () => {

  test('should throw if "dec" option is not a number', () => {
    expect(() => createFormatter({ dec: 'not-a-number' as any })).toThrow()
  })

  test('should throw if "dec" option is not a number (even if it\'s numeric)', () => {
    expect(() => createFormatter({ dec: '2' })).not.toThrow()
  })

  test('should format with given number of decimal points', () => {
    const format = createFormatter({ dec: '3' })
    const result = format(10.111111)
    expect(result).toBe('10.111')
  })

  test('should format with given number of decimal points', () => {
    const format = createFormatter({ dec: 2 })
    const result = format(10.111111)
    expect(result).toBe('10.11')
  })

})