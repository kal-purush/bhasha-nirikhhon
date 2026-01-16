var ppp = (a, b) => a + b;
console.log(ppp(100, 20));

let message = "hi";

{
  let message = "bye";
}

console.log(message);

// Destructuring Assignment
var {foo} = {foo: "Bar"};
console.log(foo);

var [,second] = ["A", "B", "C"];
console.log(second);

var people = [
  {first: "Ang", last: "Lee"},
  {first: "John", last: "Smith"}
];

var [ang] = people;
var logFirst = ({first}) => console.log(first);
logFirst(ang);
//
//var lasts = [for ({last} of people) last];
//console.log(lasts);
//
//var nums = [1, 2, 3];
//var letters = ["a", "b", "c"];
//console.log([for (num of nums) for (letter of letters) num + letter]);
//console.log([for (num of nums) [ for (letter of letters) num + letter] ]);

console.log(`${ ppp(1, 400) }`);