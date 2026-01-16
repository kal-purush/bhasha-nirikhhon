import { capitalizeFirst } from "@/utils/capitalizeFirst";

describe("Test 'capitalizeFirst' function", () => {
  test("should return passed string, with first letter capitalized", () => {
    const str = "helloworld";
    const want = "Helloworld";
    expect(capitalizeFirst(str)).toEqual(want);
  });

  test("should capitalize all of the first letters for words divided by '-' sign", () => {
    const str = "hello-world";
    const want = "Hello-World";
    expect(capitalizeFirst(str)).toEqual(want);
  });
});