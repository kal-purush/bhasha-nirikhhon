import { all_words } from "../wordlists/all_words";
import { common_words } from "../wordlists/common_words";
import { wordle_answers, wordle_guesses } from "../wordlists/wordle_list";

export type Dictionary =
  | "wordle_answers"
  | "wordle_guessable"
  | "medium"
  | "huge";

export function combineWordLists(a: string[], b: string[]): string[] {
  return [...new Set<string>([...a, ...b])];
}

export function wordify(dictionary: string, length = 5) {
  return dictionary
    .split("\n")
    .map((word) => word.trim().toLowerCase())
    .filter((word) => word.length === length);
}

export function CreateDictionary(dictionary: Dictionary, length = 5) {
  if (dictionary == "wordle_answers") {
    if (length !== 5) return [];
    return wordle_answers;
  }
  if (dictionary == "wordle_guessable") {
    if (length !== 5) return [];
    return combineWordLists(wordle_answers, wordle_guesses);
  }

  let words = wordify(common_words, length);
  if (dictionary == "huge") {
    words = combineWordLists(words, wordify(all_words, length));
  }
  if (length === 5) {
    words = combineWordLists(words, [...wordle_answers, ...wordle_guesses]);
  }

  return words;
}