import { sum } from "../src/adder"

test("returns the sum of 1 plus 2", () => {
  expect(sum(1, 2)).toBe(3)
})