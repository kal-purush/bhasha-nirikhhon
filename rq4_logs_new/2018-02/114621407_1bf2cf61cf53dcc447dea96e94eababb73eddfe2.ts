import { writing } from "./writing";

describe("writing", () => {
  it("should identify vowels", () => {
    const vowels = ["a", "e", "i", "o", "u"];
    for (const vowel of vowels) {
      expect(writing.isVowel(vowel)).toBeTruthy();
    }
  });

  it("should identify consonants", () => {
    const consonants = [
      "b",
      "c",
      "d",
      "f",
      "g",
      "h",
      "j",
      "k",
      "l",
      "m",
      "n",
      "p",
      "q",
      "r",
      "s",
      "t",
      "v",
      "w",
      "x",
      "y",
      "z"
    ];
    for (const consonant of consonants) {
      expect(writing.isConsonant(consonant)).toBeTruthy();
    }
  });
});