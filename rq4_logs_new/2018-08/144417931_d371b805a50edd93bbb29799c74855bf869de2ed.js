const reverse = array => [...array].reverse();
const compose = (a, b) => x => a(b(x));

export const flipMatrix = matrix => (
  matrix[0].map((column, index) => (
    matrix.map(row => row[index])
  ))
);

export const rotateMatrix = compose(flipMatrix, reverse);
export const flipMatrixCounterClockwise = compose(reverse, rotateMatrix);
export const rotateMatrixCounterClockwise = compose(reverse, flipMatrix);