export type LearningMode = "quiz" | "choose" | "flashcards";

export interface FlashCard {
  question: string;
  answer: string;
}

export interface WrittenQuestion {
  question: string;
  answer: string;
}