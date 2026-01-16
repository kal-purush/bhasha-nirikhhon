let TOP_CALCULATED_PRIME = 1;
const primeSet = { 1: false };

// this should only be called by `setUpPrimeSet`
function sieveOfEranthoses(top) {
  let i = 2;

  while (i <= top) {
    if (primeSet[i] === false) {
      i++;
      continue;
    }

    primeSet[i] = true;

    let j = 2;
    while (i * j <= top) {
      primeSet[i * j] = false;
      j++;
    }
    i++;
  }

  TOP_CALCULATED_PRIME = top;
  return primeSet;
}

function setUpPrimeSet(top) {
  if (top < TOP_CALCULATED_PRIME) {
    return;
  }

  sieveOfEranthoses(top);
}

function primes(top) {
  setUpPrimeSet(top);

  const primeArr = [];
  for (let num in primeSet) {
    if (num <= top && primeSet[num]) {
      primeArr.push(Number(num));
    }
  }

  return primeArr.sort((a, b) => a - b);
}

function isNumInFamilyOfSize(famSize, num) {
  const strNum = String(num);

  const digits = strNum.split('').map(str => Number(str));

  for (let i = 0; i <= 9; i++) {
    const indiciesWithThisValue = [];
    for (let idx = 0; idx < digits.length; idx++) {
      if (digits[idx] === i) {
        indiciesWithThisValue.push(idx);
      }
    }

    const allCombinationsOfTheseIndices = getAllCombos(indiciesWithThisValue);

    for (let k = 0; k < allCombinationsOfTheseIndices.length; k++) {
      const comboOfIndices = allCombinationsOfTheseIndices[k];

      if (!comboOfIndices.length) {
        continue;
      }

      let numInFamilyForThisComboOfIndices = 0;
      for (let l = 0; l <= 9; l++) {
        if (l === 0 && comboOfIndices[0] === 0) {
          continue;
        }

        const newDigits = digits.slice();

        comboOfIndices.forEach(idx => (newDigits[idx] = l));
        const newNum = Number(newDigits.join(''));

        if (primeSet[newNum]) {
          numInFamilyForThisComboOfIndices++;
        }
      }

      if (numInFamilyForThisComboOfIndices >= famSize) {
        console.log(comboOfIndices);
        return num;
      }
    }
  }
}

function getAllCombos(arr) {
  // [3]
  const combos = [];
  if (!arr.length) {
    return [[]];
  }

  const combosWithoutFirstNum = getAllCombos(arr.slice(1));
  const combosWithFirstNum = getAllCombos(arr.slice(1));
  combosWithFirstNum.forEach(combo => {
    combo.unshift(arr[0]);
  });

  return combos.concat(combosWithFirstNum, combosWithoutFirstNum);
}

const primesLessThan1000000 = primes(1000000);

primesLessThan1000000.forEach(num => {
  if (isNumInFamilyOfSize(8, num)) {
    console.log(num);
  }
});