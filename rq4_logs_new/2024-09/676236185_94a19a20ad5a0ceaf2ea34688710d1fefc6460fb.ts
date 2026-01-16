declare module 'sentiment' {
  interface SentimentResult {
    score: number;
    comparative: number;
    calculation: Array<{ [word: string]: number }>;
    tokens: string[];
    words: string[];
    positive: string[];
    negative: string[];
  }

  class Sentiment {
    analyze(
      phrase: string,
      options?: any,
      callback?: (err: any, result: SentimentResult) => void
    ): SentimentResult;
  }

  export = Sentiment;
}