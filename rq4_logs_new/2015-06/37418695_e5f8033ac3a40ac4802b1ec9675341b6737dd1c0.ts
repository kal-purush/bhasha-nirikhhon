// interface Dictionary {
//   [index: string]: string;
//   length: string;    // error, the type of 'length' is not a subtype of the indexer
// } 

// var names:Dictionary;
// names=["king"]["sheryar"];
// console.log(names["king"]);

interface StringArray {
  [index: string]: string;
}
interface Gpa {
  [index: string]: number;
}


var myArray: StringArray;
myArray={"first":"shery","second":"Fred"};
console.log(myArray["first"].length);

var gpa:Gpa={"A":4,"B+":3.5,"B":3,"C+":2.5,"C":2};

console.log("my gpa in Social Sciences is "+ gpa["A"]);