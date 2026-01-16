export function insertionSort(array: number[]) {
  if (array.length <= 1) {
    return array
  }

  for (let i = 1; i < array.length; i++) {
    let ele = array[i]
    let j = i - 1
    for (; j >= 0 && array[j] > ele; j--) {
      array[j + 1] = array[j]
    }
    array[j + 1] = ele
  }
  return array
}