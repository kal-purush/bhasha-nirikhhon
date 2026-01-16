import { Letter } from "./alphabet.ts";
import { Word } from "./dictionary.ts";


export type Test = { practice: Letter | Word } | { test: Letter | Word }
export type Result = "pass" | "hint" | "practice" | "fail"

export class UserScore {
  letterScores: {
    [phonetic: string]: Result[]
  } = {}
  wordScores: {
    [word: string]: Result[]
  } = {}  
currentScore: number = 0;

  applyResult(
    subject: Letter | Word,
    result: Result
  ) {
    if ('phonetic' in subject) {
      this.letterScores[subject.phonetic] = this.letterScores[subject.phonetic] || [];
      this.letterScores[subject.phonetic].push(result);
    } else if ('word' in subject) {
      this.letterScores[subject.word] = this.letterScores[subject.word] || [];
      this.letterScores[subject.word].push(result);
    }

    this.recomputeScore();
  }

  recomputeScore() {
    const letterScores = Object.values(this.letterScores).map(computeScore).reduce((m, a) => m + a, 0);
    const wordScores = Object.keys(this.wordScores).map((word) => {
      const results = this.wordScores[word];
      const baseScore = computeScore(results);
      const multiplier = word.length;

      return baseScore * multiplier;
    }).reduce((m, a) => m + a, 0);

    this.currentScore = letterScores + wordScores;
  }
}

export function computeScore(results: Result[]): number {
  const subset = results.slice(-5);

  // 2 points for every pass
  // 1 point for every non-fail

  return subset.reduce((m, a) => (a == "pass" ? 2 : a == "fail" ? 0 : 1) + m, 0);
}