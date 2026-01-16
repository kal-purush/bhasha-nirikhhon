function solution(cell: string): number {
  const charCode = cell[0].charCodeAt(0);
  const number = parseInt(cell[1]);
  let cont = 0;

  if (charCode - 2 >= 97) {
    if (number - 1 >= 1) cont++;
    if (number + 1 <= 8) cont++;
  }

  if (charCode + 2 <= 104) {
    if (number - 1 >= 1) cont++;
    if (number + 1 <= 8) cont++;
  }

  if (number - 2 >= 1) {

    if (charCode - 1 >= 97) cont++;
    if (charCode + 1 <= 104) cont++;
  }

  if (number + 2 <= 8) {
    if (charCode - 1 >= 97) cont++;
    if (charCode + 1 <= 104) cont++;
  }

  return cont;

}
solution('a1');