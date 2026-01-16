export interface Game {
  id: string;
  title: string;
  releaseDate: Date;
  genre: string;
  rating: string;
}

export const gamesList: Game[] = [];