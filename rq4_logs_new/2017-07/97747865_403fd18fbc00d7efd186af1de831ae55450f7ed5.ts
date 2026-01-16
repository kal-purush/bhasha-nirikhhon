import { prettyNumber } from './text-formatting'

describe('text-formatting service', () => {
  describe('prettyNumber', () => {
    describe('input values less than 1,000', () => {
      it('should format integers', () => {
        expect(prettyNumber(567)).toBe('567')
      })

      it('should round decimals down to the nearest integer', () => {
        expect(prettyNumber(43.499)).toBe('43')
      })

      it('should round decimals up to the nearest integer', () => {
        expect(prettyNumber(43.50001)).toBe('44')
      })
    })

    describe('input values greater than 1,000', () => {
      it('should format integers', () => {
        expect(prettyNumber(1455567)).toBe('1,455,567')
      })

      it('should round decimals down to the nearest integer', () => {
        expect(prettyNumber(5389143.499)).toBe('5,389,143')
      })

      it('should round decimals up to the nearest integer', () => {
        expect(prettyNumber(5389143.50001)).toBe('5,389,144')
      })
    })
  })
})