// https://eloquentjavascript.net/02_program_structure.html#i_swb9JBtSQQ

function makeBoard(size) {
  let board = "";
  for (let i = 0; i < size; i++) {
    if (i > 0) {
      board += "\n";
    }
    for (let j = 0; j < size; j++) {
      if ((i + j) % 2 !== 0) {
        board += "#";
      } else {
        board += " ";
      }
    }
  }
  return board;
}