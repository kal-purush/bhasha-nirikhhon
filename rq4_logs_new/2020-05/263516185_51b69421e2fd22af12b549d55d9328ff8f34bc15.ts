export const PENDING = 'PENDING'

// Ranking between all of the players. Default is 0.
export interface Ranking {
  [key: string]: number;
}

// Stores
// - Event state
// - Mapping from name to score
// - active player
export interface MusictionaryState {
  state?: string;
  activePlayer?: string;
  ranking?: Ranking;
}