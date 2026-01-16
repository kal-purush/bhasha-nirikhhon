function findMaxSubArraySum(arr, k) {
  if (k <= 0 || k > arr.length) {
    throw new Error("Invalid value of k");
  }

  let maxSum = 0;
  let currentSum = 0;

  // hitung jumlah maksimum subarray
  for (let i = 0; i < k; i++) {
    currentSum += arr[i];
  }

  maxSum = currentSum;

  for (let i = k; i < arr.length; i++) {
    currentSum += arr[i] - arr[i - k]; // tambahkan elemen baru dan kurangi elemen pertama dari subarray

    maxSum = Math.max(maxSum, currentSum); // perbarui nilai maksimum
  }
  return maxSum;
}

module.exports = findMaxSubArraySum;