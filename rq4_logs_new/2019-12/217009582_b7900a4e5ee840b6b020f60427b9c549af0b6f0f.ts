import { createFormatter } from '../../src'

describe('format round option', () => {

  test('should throw if "dec" sub-option is not a number', () => {
    expect(() => createFormatter({ round: { dec: 'not-a-number' } })).toThrow()
  })

  test('should not throw if "dec" sub-option is not a numeric string', () => {
    expect(() => createFormatter({ round: { dec: '2' } })).not.toThrow()
  })

  test('should default to 4 decimal', () => {
    const format = createFormatter({})
    const result = format(10.111111)
    expect(result).toBe('10.1111')
  })

  test('should format with given number of decimal points using number', () => {
    const format = createFormatter({ dec: 2 })
    const result = format(10.111111)
    expect(result).toBe('10.11')
  })

  test('should format with given number of decimal points using numeric string', () => {
    const format = createFormatter({ round: { dec: '3' } })
    const result = format(10.111111)
    expect(result).toBe('10.111')
  })

  test('should return with fixed number of decimal points', () => {
    const format = createFormatter({ round: { fixed: true } })
    const result = format(11)
    expect(result).toBe('11.0000')
  })

  test('should return with fixed number of decimal points (with unit prefix)', () => {
    const format = createFormatter({ round: { fixed: true } })
    const result = format(123e-3)
    expect(result).toBe('123.0000 m')
  })

  test('should return with fixed number of decimal points (with unit)', () => {
    const format = createFormatter({ unit: 'g', round: { fixed: true } })
    const result = format(123e-3)
    expect(result).toBe('123.0000 mg')
  })

  test('should use custom rounder', () => {
    const round = (num: number): string => `${Math.round(num)}`
    const format = createFormatter({ unit: 'g', round })
    const result = format(123.7e-3)
    expect(result).toBe('124 mg')
  })

})