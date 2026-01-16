import { createFormatter } from '../../src'

describe('format unit option', () => {

  test('should format using given unit', () => {
    const format = createFormatter({ unit: 'g' })
    const result = format(10)
    expect(result).toBe('10 g')
  })

  test('should format zero using given unit', () => {
    const format = createFormatter({ unit: 'g' })
    const result = format(0)
    expect(result).toBe('0 g')
  })

  test('should format using given unit with prefix', () => {
    const format = createFormatter({ unit: 'g' })
    const result = format(10e3)
    expect(result).toBe('10 Kg')
  })

})