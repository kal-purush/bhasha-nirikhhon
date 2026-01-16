import { integer } from './util'

describe('integer', () => {
  class Test {
    @integer
    public foo
  }

  it('sholud make property\'s initial value undefined', () => {
    const test = new Test()

    expect(test.foo).toBe(undefined)
  })

  it('sholud allow integer assignment', () => {
    const test = new Test()

    test.foo = 3
    expect(test.foo).toBe(3)
  })

  it('should throw for non-integer assignment', () => {
    const test = new Test()
    const errorMsg = '`foo` must be an integer.'

    expect(() => {
      test.foo = 3.4
    }).toThrow(errorMsg)

    expect(() => {
      test.foo = null
    }).toThrow(errorMsg)

    expect(() => {
      test.foo = NaN
    }).toThrow(errorMsg)
  })

  it('should not change each other\'s value', () => {
    const testA = new Test()
    const testB = new Test()

    testA.foo = 7
    testB.foo = 5

    expect(testA.foo).toBe(7)
    expect(testB.foo).toBe(5)
  })
})