function add(arr) {
  // initialise sum of even and odd numbers to zero
  let oddSum = 0;
  let evenSum = 0;

  //   map through array
  arr.map((num) => {
    // check if number is divisible by 2 without reminders(even number)
    if (num % 2 == 0) {
      evenSum += num;
    } else {
      oddSum += num;
    }
  });

  return [evenSum, oddSum];
}

add([2, 3, 7, 4]);