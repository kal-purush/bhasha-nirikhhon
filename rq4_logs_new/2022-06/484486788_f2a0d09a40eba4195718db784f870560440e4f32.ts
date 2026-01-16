import { regexList } from "./regex-list";

export const buildPageContent = (words: string[]) => {
  let chars: string[] = [];
  const pageContent: string[][] = [];

  for (let word of words) {
    if (word === "↵") {
      pageContent.push(chars);
      chars = [];
      continue;
    }

    Object.values(regexList).forEach(
      ({ re, replacedBy }) => (word = word.replace(re, replacedBy))
    );

    for (let w of word) {
      if (w === "”") {
        chars.push('"');
        continue;
      }

      chars.push(w);
    }
    chars.push(" ");
  }

  return pageContent;
};