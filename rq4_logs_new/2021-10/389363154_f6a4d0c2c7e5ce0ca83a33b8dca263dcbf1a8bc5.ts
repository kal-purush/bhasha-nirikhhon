import { User } from './user';

export interface Kweet {
  kwitter: User;
  content: string;
  likes: number;
  kweetedAt: string;
}