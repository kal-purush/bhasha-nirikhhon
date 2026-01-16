const {
  addAllNumbers,
  doubleAllNumbers,
  getAllEvenNumbers,
  findNumberByValue,
  allNumbersAreNegative,
  getLargestNumber,
} = require("../src/00-numbers");

describe("addAllNumbers()", () => {
  test("it should add up all the numbers into a single number", () => {
    const scenario1 = addAllNumbers([10, 20, 30]);
    expect(scenario1).toEqual(60);

    const scenario2 = addAllNumbers([-10, 20, -30]);
    expect(scenario2).toEqual(-20);
  });

  test("it return `0` if the array is empty", () => {
    const scenario = addAllNumbers([]);
    expect(scenario).toEqual(0);
  });
});

describe("doubleAllNumbers()", () => {
  test("it should double all of the numbers in the array and return an array", () => {
    const scenario1 = doubleAllNumbers([2, 4, 6]);
    expect(scenario1).toEqual([4, 8, 12]);

    const scenario2 = doubleAllNumbers([1, 3, 7]);
    expect(scenario2).toEqual([2, 6, 14]);

    const scenario3 = doubleAllNumbers([3, 0, -3]);
    expect(scenario3).toEqual([6, 0, -6]);
  });

  test("it should return an empty array if the original array is empty", () => {
    const scenario = doubleAllNumbers([]);
    expect(scenario).toEqual([]);
  });
});

describe("getAllEvenNumbers()", () => {
  test("it should return an array of only the even numbers in the original array", () => {
    const scenario1 = getAllEvenNumbers([1, 2, 3, 4]);
    expect(scenario1).toEqual([2, 4]);

    const scenario2 = getAllEvenNumbers([1, 3, 5]);
    expect(scenario2).toEqual([]);

    const scenario3 = getAllEvenNumbers([10, 20, 30]);
    expect(scenario3).toEqual([10, 20, 30]);
  });

  test("it should return an empty array if the original array is empty", () => {
    const scenario = getAllEvenNumbers([]);
    expect(scenario).toEqual([]);
  });
});

describe("findNumberByValue()", () => {
  test("it should return the given number if it is in the array", () => {
    const scenario1 = findNumberByValue([10, 20, 30], 10);
    expect(scenario1).toEqual(10);

    const scenario2 = findNumberByValue([1, 5, 9], 1);
    expect(scenario2).toEqual(1);
  });

  test("it should return `null` if the number cannot be found", () => {
    const scenario1 = findNumberByValue([10, 20, 30], 11);
    expect(scenario1).toEqual(null);

    const scenario2 = findNumberByValue([], 1);
    expect(scenario2).toEqual(null);
  });
});

describe("allNumbersAreNegative()", () => {
  test("it should return `true` if all of the numbers in the array are negative", () => {
    const scenario = allNumbersAreNegative([-10, -1, -3]);
    expect(scenario).toEqual(true);
  });

  test("it should return `false` if all of the numbers in the array are negative", () => {
    const scenario = allNumbersAreNegative([-10, 1, -3]);
    expect(scenario).toEqual(false);
  });

  test("it should ignore `0` in the evaluation of positive or negative numbers", () => {
    const scenario1 = allNumbersAreNegative([-10, 0, -3]);
    expect(scenario1).toEqual(true);

    const scenario2 = allNumbersAreNegative([10, 0, -3]);
    expect(scenario2).toEqual(false);
  });

  test("it should return `false` if the array is empty", () => {
    const scenario = allNumbersAreNegative([]);
    expect(scenario).toEqual(false);
  });
});

describe("getLargestNumber()", () => {
  test("it should return the largest number in the array", () => {
    const scenario1 = getLargestNumber([10, 20, 30]);
    expect(scenario1).toEqual(30);

    const scenario2 = getLargestNumber([10, 35, 20, 45, 30]);
    expect(scenario2).toEqual(45);

    const scenario3 = getLargestNumber([-3, -2, 0, -1]);
    expect(scenario3).toEqual(0);

    const scenario4 = getLargestNumber([-3, -2, -1]);
    expect(scenario4).toEqual(-1);
  });

  test("it should return `null` if the array is empty", () => {
    const scenario = getLargestNumber([]);
    expect(scenario).toEqual(null);
  });
});