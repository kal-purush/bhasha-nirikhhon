import { swap } from "./swap";

export const sortingChoice = (arr: number[], order: string) => {
  for (let i = 0; i < arr.length - 1; i++) {
    let compareIndex = i;
    for (let j = i + 1; j < arr.length; j++) {
      const shouldSwap =
        order === "descending"
          ? arr[j] > arr[compareIndex]
          : arr[j] < arr[compareIndex];

      if (shouldSwap) {
        compareIndex = j;
      }
    }

    if (compareIndex !== i) {
      swap(arr, i, compareIndex);
    }
  }
  return arr;
};

export const sortingBubble = (arr: number[], order: string) => {
  for (let i = 0; i < arr.length - 1; i++) {
    for (let j = 0; j < arr.length - i - 1; j++) {
      const shouldSwap =
        order === "descending" ? arr[j] < arr[j + 1] : arr[j] > arr[j + 1];
      if (shouldSwap) {
        swap(arr, j, j + 1)
      }
    }
   
  }
  return arr;
};