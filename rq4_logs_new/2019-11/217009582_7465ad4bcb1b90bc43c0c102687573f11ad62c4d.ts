import { createFormatter } from "../../src";

describe("format dec option", () => {

  test('should throw if "dec" option is not a number', () => {
    expect(() => createFormatter({ dec: "not-a-number" as any })).toThrow();
  });

  test(`should throw if "dec" option is not a number (even if it\'s numeric)`, () => {
    // will be fixed soon, this is a known issue
    expect(() => createFormatter({ dec: "2" as any })).toThrow();
  });

  test("should format with given number of decimal points", () => {
    const format = createFormatter({ dec: 2 });
    const result = format(10.111111);
    // will be fixed soon, this is a known issue
    // if no unit provided should return without extra space in the end
    expect(result).toBe("10.11 ");
  });

});