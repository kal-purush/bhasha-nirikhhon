import mergeRanges from './mergeRanges'

describe('mergeRanges', () => {
  it('merges all overlapping ranges', () => {
    const ranges = [
      { start: 0, end: 1 },
      { start: 3, end: 5 },
      { start: 4, end: 8 },
      { start: 10, end: 12 },
      { start: 9, end: 10 },
    ]

    expect(mergeRanges(ranges)).toEqual([
      { start: 0, end: 1},
      { start: 3, end: 8 },
      { start: 9, end: 12 },
    ])
  })

  it('merges touching ranges', () => {
    const ranges = [
      { start: 1, end: 2 },
      { start: 2, end: 3 },
    ]

    expect(mergeRanges(ranges)).toEqual([{ start: 1, end: 3 }])
  })

  it('merges a range contained by an existing range', () => {
    const ranges = [
      { start: 1, end: 5 },
      { start: 2, end: 3 },
    ]

    expect(mergeRanges(ranges)).toEqual([{ start: 1, end: 5 }])
  })

  it('merges more than two ranges into one range', () => {
    const ranges = [
      { start: 1, end: 10 },
      { start: 2, end: 6 },
      { start: 3, end: 5 },
      { start: 7, end: 9 },
    ]

    expect(mergeRanges(ranges)).toEqual([{ start: 1, end: 10 }])
  })
})