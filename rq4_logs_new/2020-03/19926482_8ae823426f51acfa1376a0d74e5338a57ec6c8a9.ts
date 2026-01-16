import { isAnagram, isAnagramSpaceEfficient } from './isAnagram'

describe('isAnagram()', () => {
  it('returns true for a string that is an anagram', () => {
    expect(isAnagram('anagram', 'nagaram')).toBe(true)
  })

  it('returns false for a string that is not an anagram', () => {
    expect(isAnagram('car', 'cat')).toBe(false)
  })
})

describe('isAnagramSpaceEfficient()', () => {
  it('returns true for a string that is an anagram', () => {
    expect(isAnagramSpaceEfficient('anagram', 'nagaram')).toBe(true)
  })

  it('returns false for a string that is not an anagram', () => {
    expect(isAnagramSpaceEfficient('car', 'cat')).toBe(false)
  })
})