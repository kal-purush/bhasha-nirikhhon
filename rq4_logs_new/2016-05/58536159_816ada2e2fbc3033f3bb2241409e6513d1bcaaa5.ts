import Board from '../logics/Board';
import _Number from '../logics/Number';
import Row from '../logics/Row';

function row(color: string, numberLabels: number[]): Row {
  const row = new Row();
  for (const numberLabel of numberLabels) {
    row.push(new _Number(color, numberLabel));
  }
  return row;
}

function red(): Row {
  return row('red', [10, 6, 2, 8, 3, 4, 12, 5, 9, 7, 11]);
}

function yellow(): Row {
  return row('yellow', [9, 12, 4, 6, 7, 2, 5, 8, 11, 3, 10]);
}

function green(): Row {
  return row('green', [8, 2, 10, 12, 6, 9, 7, 4, 5, 11, 3]);
}

function blue(): Row {
  return row('blue', [5, 7, 11, 9, 12, 3, 8, 10, 2, 6, 4]);
}

export default function mixedNumbers(board: Board): Board {
  board.setRows([red(), yellow(), green(), blue()]);
  board.setFails(4);
  return board;
}