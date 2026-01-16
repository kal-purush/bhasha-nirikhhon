function solution(inputString: string): number {
  const patt = /[^0-9]/;
  const numberSplit :string[] = inputString.split(patt);
  const numbers:string[] = numberSplit.filter(number => parseInt(number));
  const sum:number = numbers.reduce((partialSum, a) => partialSum + parseInt(a), 0);
  return sum;
}
solution('2 apples, 12 oranges');