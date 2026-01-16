// Find the only repetitive element between 1 to n-1
function findDuplicates(array = []): [] {

	if (!array?.length){
		return [];
	}


  const result = [];

  for (let i = 0; i < array.length; i += 1) {
    for (let j = 0; j < array.length, i !== j; j += 1) {
      if (array[i] === array[j]) {
	 result.push(array[i]);
	}
    }
  }

  return result;
};

const array = [4,3,2,7,8,2,3,1];

console.log(findDuplicates(array));


function findRepeating(array = []): number {

	if (!array?.length){
		return 0;
	}

	let sum = 0;
	for (let i = 0; i < array.length; i += 1) {
		sum += array[i];
	}

	return sum - ((array.length - 1 * array.length) / 2);
}


console.log(findRepeating(array));