import { IUserRef } from "./user";
import { ISauceRef } from "./sauce";

export interface IReviewSection {
  rating: number;
  txt: string;
}

// Declare state types with `readonly` modifier to get compile time immutability.
// https://github.com/piotrwitek/react-redux-typescript-guide#state-with-type-level-immutability
export interface IReview {
  _id: number;
  author: IUserRef; // User reference
  sauce: ISauceRef;
  created: Date;
  overall: IReviewSection; // Only review bit that is required
  label?: IReviewSection;
  aroma?: IReviewSection;
  taste?: IReviewSection;
  heat?: IReviewSection;
  note?: IReviewSection;
}

// Trimmed down for reference only
export interface IReviewRef {
  _id: number;
}