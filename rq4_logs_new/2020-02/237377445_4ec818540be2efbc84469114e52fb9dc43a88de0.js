{
  /*
    Create a function getRemainder that takes two numbers and returns the remainder (value left over from division) of dividing the larger number by the smaller one.
 */
}

function getRemainder(num1, num2) {
  //your co//var result = 0;
  if (num1 > num2) {
    var result = num1 % num2;
  } else {
    result = num2 % num1;
  }
  return result;
}
//Uncomment the lines below to test your code

console.log(getRemainder(17, 5)); //=> 2
console.log(getRemainder(20, 5)); //=> 0
console.log(getRemainder(8, 22)); //=> 6
console.log(getRemainder(7, 42)); //=> 0

//GRADE CALCULATOR
{
  /*
Create a function gradeCalculator which takes a grade (number) and returns its letter grade.
grades 90 and above should return "A"
grades 80 to 89 should return "B"
grades 70 to 79 should return "C"
grades 60 to 69 should return "D"
59 and below should return "F"
 */
}
function gradeCalculator(gradeData) {
  //your code goes here...
  if (gradeData >= 90) {
    return "A";
  }
  if (gradeData >= 80) {
    return "B";
  }
  if (gradeData >= 70) {
    return "C";
  }
  if (gradeData >= 60) {
    return "D";
  }
  if (gradeData <= 59) {
    return "F";
  }
}
//Uncomment the lines below to test your code
console.log(gradeCalculator(92)); //=> "A"
console.log(gradeCalculator(84)); //=> "B"
console.log(gradeCalculator(70)); //=> "C"
console.log(gradeCalculator(61)); //=> "D"
console.log(gradeCalculator(43)); //=> "F"