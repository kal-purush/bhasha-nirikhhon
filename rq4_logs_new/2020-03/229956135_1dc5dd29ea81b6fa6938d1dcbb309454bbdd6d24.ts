import { ResultParser, ZeroParser, ItemParser } from "../src/primitiveParsers";
import { IndexedStringSource } from "../src/indexedStringSource";

describe("ResultParser", () => {
  it("should return the given value without consuming input", () => {
    const res = ResultParser("input")(new IndexedStringSource("abcde"));
    expect(res).toEqual([["input", new IndexedStringSource("abcde")]]);
  });
});

describe("ZeroParser", () => {
  it("should not return any results", () => {
    const res = ZeroParser()(new IndexedStringSource("abcde"));
    expect(res).toEqual([]);
  });
});

describe("ItemParser", () => {
  it("should return the pair of the first letter and the rest.", () => {
    const res = ItemParser(new IndexedStringSource("abcde"));
    expect(res).toEqual([["a", new IndexedStringSource("abcde").tail]]);
  });

  it("should not return any results if the source is empty.", () => {
    const res = ItemParser(new IndexedStringSource(""));
    expect(res).toEqual([]);
  });
});