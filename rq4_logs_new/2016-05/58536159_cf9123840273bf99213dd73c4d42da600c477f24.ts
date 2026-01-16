import Board from '../logics/Board';
import { Ascending, Descending } from './classic';

export default function classic(board: Board): Board {
  const red = Ascending('red');
  const yellow = Ascending('yellow');
  const green = Descending('green');
  const blue = Descending('blue');

  const redYellow = Ascending('red-yellow')
    .enableBigPoints()
    .setLinkedRows([red, yellow]);

  const greenBlue = Descending('green-blue')
    .enableBigPoints()
    .setLinkedRows([green, blue]);

  return board
    .setRows([red, redYellow, yellow, green, greenBlue, blue])
    .setFails(4);
}