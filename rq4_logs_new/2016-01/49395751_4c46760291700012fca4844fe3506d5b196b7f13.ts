import * as readline from 'readline';
import Suit from './suit';
import Card from './card';
import Cell from './cell';
import Game from './game';

class Application {
  run(): void {
    console.log('Game Start');
    this._mainLoop()
    .then(() => console.log('Game Over'));
  }

  _input(message: string): Promise<string> {
    return new Promise((resolve, reject) => {
      const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
      });
      rl.question(message, (value) => {
        resolve(value);
        rl.close();
      });
    });
  }

  _mainLoop(game?: Game): Promise<Game> {
    return Promise.resolve(game ? game : Game.new())
    .then(game => {
      const errorHandler = (error: Error) => {
        console.error(error);
        return game;
      };
      this._render(game);
      if (game.isSelected()) {
        return this._input('move / unselect [ -15] ? ')
        .then(dst =>
          dst === '' ? game.unselect() : game.moveTo(parseInt(dst, 10))
        )
        .catch(errorHandler);
      } else {
        return this._input('select [0-11] ? ')
        .then(src => game.select(parseInt(src, 10)))
        .catch(errorHandler);
      }
    })
    .then(game => game.isOver() ? null : this._mainLoop(game));
  }

  _render(game: Game): void {
    const cursor = game.getCursor();
    this._renderFreeAndHomeCells(
      game.getFreeCells(), game.getHomeCells(), cursor
    );
    this._renderColumns(game.getColumns(), cursor);
  }

  _toString(card: Card, defaultString?: string): string {
    if (!card) return defaultString;
    const { suit, number } = card;
    const suitSymbols: any = {};
    suitSymbols[Suit.SPADE] = 'S';
    suitSymbols[Suit.HEART] = 'H';
    suitSymbols[Suit.CLUB] = 'C';
    suitSymbols[Suit.DIAMOND] = 'D';
    const symbol: string = suitSymbols[suit];
    const numberString = '' + (number < 10 ? '0' + number : number);
    const cardString = symbol + numberString;
    return cardString;
  }

  _renderColumnsHeader(): void {
    const items: string[] = [];
    for (let x = 0; x < 8; x++) {
      items.push('  ' + x + '  ');
    }
    const line = items.join(' ');
    console.log(line);
  }

  _renderColumns(columns: Card[][], cursor: number): void {
    this._renderColumnsHeader();
    const max = columns.reduce((max, i) => i.length > max ? i.length : max, 0);
    const column = columns.filter((_, x) => x === cursor)[0];
    for (let y = 0; y < max; y++) {
      const isBottom = column && column.filter(i => !!i).length === y + 1;
      const items: Card[] = [];
      for (let x = 0; x < 8; x++) {
        items.push(columns[x][y]);
      }
      const str = (card: Card) => this._toString(card, '   ');
      const wrap = (s: string, p: boolean): string => p ? `[${s}]` : ` ${s} `;
      const line = items
      .map((card, index) => wrap(str(card), (index === cursor && isBottom)))
      .join(' ');
      console.log(line);
    }
    console.log(''); // blank line
  }

  _renderFreeAndHomeCells(
    freeCells: Cell[],
    homeCells: Cell[],
    cursor: number
  ): void {
    const fstr = (card: Card): string => this._toString(card, '___');
    const wrap = (s: string, p: boolean): string => p ? `[${s}]` : ` ${s} `;
    const s = (i: number): string => ['S', 'H', 'C', 'D'][i];
    const hstr = (card: Card, symbol: string): string =>
      this._toString(card, symbol + '__');
    console.log('  8     9     10    11 |  12    13    14    15  ');
    const fs = freeCells
    .map(i => i.getCard())
    .map((card, index) => wrap(fstr(card), (cursor === index + 8)))
    .join(' ');
    const hs = homeCells
    .map(i => i.getCard())
    .map((card, index) => wrap(hstr(card, s(index)), (cursor === index + 12)))
    .join(' ');
    const line = fs + '|' + hs;
    console.log(line);
    console.log(''); // blank line
  }
}

export default Application;