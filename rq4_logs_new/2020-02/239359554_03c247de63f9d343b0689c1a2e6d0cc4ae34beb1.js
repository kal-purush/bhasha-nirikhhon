export function mergeSort(unsortedArray) {
  if (unsortedArray.length <= 1) {
    return unsortedArray;
  }
  const middle = Math.floor(unsortedArray.length / 2);
  const left = unsortedArray.slice(0, middle);
  const right = unsortedArray.slice(middle);

  return merge(mergeSort(left), mergeSort(right));
}

function merge(leftArray, rightArray) {
  let resultArray = [];
  let leftIdx = 0, rightIdx = 0;

  while (leftIdx < leftArray.length && rightIdx < rightArray.length) {
    if (leftArray[leftIdx] < rightArray[rightIdx]) {
      resultArray.push(leftArray[leftIdx]);
      leftIdx++;
    } else {
      resultArray.push(rightArray[rightIdx]);
      rightIdx++;
    }
  }
  return resultArray
    .concat(leftArray.slice(leftIdx))
    .concat(rightArray.slice(rightIdx));
}