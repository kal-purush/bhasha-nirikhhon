import { Schema } from "mongoose";

interface Answer {
  _id: Schema.Types.ObjectId;
  text: string;
  isCorrect: boolean;
}

interface Question {
  _id: Schema.Types.ObjectId;
  question: string;
  answers: Answer[];
}

export type { Question };