export const rangeMap = (count = 0, predicate: (...a: any[]) => {}) =>
  Array(count)
    .fill(0)
    .map(predicate);