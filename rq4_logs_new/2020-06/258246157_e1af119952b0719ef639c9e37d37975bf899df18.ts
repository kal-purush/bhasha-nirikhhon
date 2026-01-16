import { Book, Player } from '../../types/models';

const getNextPlayer = (current: number, players: Array<Player>): Player =>
  current === players.length ? players[0] : players[current];

export const hasAllBooks = (
  books: Array<Book>,
  players: Array<Player>
): boolean =>
  players.every((player) => {
    books.some((book) => book.owner.id === player.id);
  });

export const assignPlayers = (
  books: Array<Book>,
  players: Array<Player>
): Array<Book> => {
  return books.map((book, i) => ({
    ...book,
    pages: [{ startWord: book.startWord, drawer: getNextPlayer(i, players) }],
  }));
};