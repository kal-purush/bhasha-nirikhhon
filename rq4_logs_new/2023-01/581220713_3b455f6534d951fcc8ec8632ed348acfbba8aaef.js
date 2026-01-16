console.log(`To find vowels in given string: \n`);
string= "I am very good IT Developer"
count=0
for (let index = 0; index < string.length; index++) {
    var eachChar= string.charAt(index)
    let eachCharInLow= eachChar.toLowerCase()
    // console.log(eachCharInLow);
    if (eachCharInLow== "a" || eachCharInLow=="e" || eachCharInLow== "i" || eachCharInLow== "o" || eachCharInLow== "u"  ) {
        count=1+count
    }
}
console.log(`Given string ==> ${string}`);
console.log(`The total number of vowels in the given string ==> ${count}`);
console.log(`==================================================================`);
console.log(`To find sum of cubes from 1 to 5: \n`);
function sumOfCubes(){
counter= 0;
for (let index = 1; index <=5; index++) {
    let multi= index*index*index
    //console.log(multi);
    counter= counter+multi
}
console.log(`The sum of cubes from 1 to 5 ==> ${counter}`);
}
sumOfCubes()
console.log(`==================================================================`);
console.log(`To find odd positioned characters from a given string: \n`);

var oddPositionedChar = function (string) {
    var stringLength=string.length
    var newStr = ""
    for (let index = 0; index < stringLength; index++) {
      if (index%2==1) {
        oddChar = string.charAt(index);
        newStr = newStr.concat(oddChar);
        newStr = newStr.split("").join("")
      }
    }
    console.log(`The given string is= "${string}"`);
    console.log(`Odd positioned characters without considering space is= "${newStr}"`);
  }
  oddPositionedChar("Hard work always pays back");
  oddPositionedChar("Soon i will be Angular IT champ");


