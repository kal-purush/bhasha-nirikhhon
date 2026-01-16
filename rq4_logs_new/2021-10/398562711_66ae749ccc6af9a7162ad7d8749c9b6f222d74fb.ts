import type { Testcase } from '@tests/utils/types'
import testSubject from '../inspectable.util'

/**
 * @file Unit Tests - inspectable
 * @module log/utils/tests/unit/inspectable
 */

describe('unit:utils/inspectable', () => {
  type Case = Testcase<boolean> & { state: string; value: any }

  const cases: Case[] = [
    {
      expected: false,
      state: 'value is boolean',
      value: true
    },
    {
      expected: false,
      state: 'value is undefined',
      value: undefined
    },
    {
      expected: false,
      state: 'value is number',
      value: 13
    },
    {
      expected: false,
      state: 'value is string',
      value: 'string'
    },
    {
      expected: true,
      state: 'value is array',
      value: []
    },
    {
      expected: true,
      state: 'value is function',
      value: () => ({})
    },
    {
      expected: true,
      state: 'value is null',
      value: null
    },
    {
      expected: true,
      state: 'value is object',
      value: {}
    }
  ]

  it.each<Case>(cases)('should return $expected if $state', testcase => {
    // Arrange
    const { expected, value } = testcase

    // Act + Expect
    expect(testSubject(value)).toBe(expected)
  })
})