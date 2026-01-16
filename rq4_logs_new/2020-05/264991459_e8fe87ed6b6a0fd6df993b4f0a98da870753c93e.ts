const percentage = (index: number, length: number) => {
  return index / Math.max(length - 1, 1);
};
export default percentage;