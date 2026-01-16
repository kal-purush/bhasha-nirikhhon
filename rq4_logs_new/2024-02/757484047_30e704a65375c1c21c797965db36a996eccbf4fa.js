const {
  cityIndex,
  citySubstring,
  cityReplaced,
  cityStartsWith,
  cityEndsWith,
  shoppingListArray,
  cityStrConcat
} = require('../../src/advanced/strings-adv.js')

describe('Advanced strings', () => {
  it('Check city index', () => {
    expect(cityIndex).toBe(4)
  })
  it('Check city substring', () => {
    expect(citySubstring).toBe('York')
  })
  it('Check replace city name', () => {
    expect(cityReplaced).toBe('New Delhi')
  })
  it('Check starts with', () => {
    expect(cityStartsWith).toBe(true)
  })
  it('Check ends with', () => {
    expect(cityEndsWith).toBe(true)
  })
  it('Check shopping list', () => {
    expect(shoppingListArray).toEqual([
      'apples',
      'bananas',
      'oranges',
      'grapes'
    ])
  })
  it('Check undefined', () => {
    expect(cityStrConcat).toBe('New York City')
  })
})