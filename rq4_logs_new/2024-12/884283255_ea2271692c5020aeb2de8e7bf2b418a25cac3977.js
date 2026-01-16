const numbers = [2, 2, 3, 1, 5, 6, 6, 8];

const counts = {};
numbers.forEach(num => {
    counts[num] = (counts[num] || 0) + 1;
});

console.log(counts); 
console.log(counts[3]); 



numbers.forEach(num => {
    counts[num] = (counts[num] || 0) + 1;
});

// Loop through the counts object and log each number's count one by one
for (let num in counts) {
    // console.log(`The number ${num} is used ${counts[num]} times.`);
    // console.log( `${num} : ${counts[num]}` );
}

const categories = [...new Set(numbers.map((num) => num))];
// console.log(categories);
// console.log(categories.indexOf(3));