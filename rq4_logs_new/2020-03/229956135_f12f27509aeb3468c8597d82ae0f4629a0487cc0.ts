import { Seq } from "../src/parserCombinators";
import { Source } from "../src/source";
import { IndexedStringSource } from "../src/indexedStringSource";

describe("Seq", () => {
  it("should apply one parser after another", () => {
    const oneTwoThreeParser = (input: Source): [number, Source][] =>
      input.currentString == "123ab"
        ? [
            [1, new IndexedStringSource("23ab")],
            [12, new IndexedStringSource("3ab")],
            [123, new IndexedStringSource("ab")],
          ]
        : [];
    const abParser = (input: Source): [string, Source][] =>
      input.currentString == "ab"
        ? [
            ["", new IndexedStringSource("ab")],
            ["a", new IndexedStringSource("b")],
            ["ab", new IndexedStringSource("")],
          ]
        : [];

    expect(
      Seq(oneTwoThreeParser, abParser)(new IndexedStringSource("123ab")),
    ).toEqual([
      [[123, ""], new IndexedStringSource("ab")],
      [[123, "a"], new IndexedStringSource("b")],
      [[123, "ab"], new IndexedStringSource("")],
    ]);
  });
});