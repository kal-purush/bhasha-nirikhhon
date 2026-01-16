const companies = ["Google", "Facebook", "Uber"];

const common = companies;

// Destucuring element of passed aaray
const [name, ...rest] = companies;

// Destructuring property of an element
const { length } = companies;

console.log(common);
console.log(rest);
console.log(name);
console.log(length);