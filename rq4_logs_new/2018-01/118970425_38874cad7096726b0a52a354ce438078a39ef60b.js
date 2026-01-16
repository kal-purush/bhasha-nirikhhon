console.log("grades.js is connected");

const scores = [82, 71, 62, 95, 55, 98, 69, 72, 78, 84, 64, 58, 87, 60]
const grades = {} // You'll need to change this line of code

var gradeA = 0;
var gradeB = 0;
var gradeC = 0;
var gradeD = 0;
var gradeF = 0;


for (let i = 0; i < scores.length; i++) {
    if(scores[i] >= 91){
        gradeA += 1;
    }else if(scores[i] <= 90 && scores[i] >= 81){
        gradeB += 1;
    }else if(scores[i] <= 80 && scores[i] >= 71){
        gradeC += 1;
    }else if(scores[i] <= 70 && scores[i] >= 61){
        gradeD += 1;
    }else if(scores[i] <= 60){
        gradeF += 1;
    };
}
console.log("Grade Totals Below")
console.log("Total As: ", gradeA);
console.log("Total Bs: ", gradeB); 
console.log("Total Cs: ", gradeC);
console.log("Total Ds: ", gradeD);
console.log("Total Fs: ", gradeF);

console.log("Total Grades", scores.length)

scores.sort();
console.log(`The lowest grade is ${scores[0]}.`);
console.log(`The highest grade is ${scores[13]}.`);

// Use console.log() to output the following criteria to the browser console.

// How many of each grade? Accomplish this with a for..in loop.
// What is the lowest score? Sort the array and find out.
// What is the highest score?
// Which grade had the most students achieve it? Use an if statment, and a currentGradeCount variable, in your for..in loop to see if the current grade's value is higher than the last one.
// Which grade had the fewest students achieve it?