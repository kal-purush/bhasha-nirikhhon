// this time, find who's passed. 
// Suppose that the passing grade is 60.

const students = [
    {name: "a", grade: 0},
    {name: "b", grade: 10},
    {name: "c", grade: 20},
    {name: "d", grade: 40},
    {name: "e", grade: 50},
    {name: "f", grade: 70},
    {name: "g", grade: 80},
    {name: "h", grade: 100}
];

const passingStudents = students.filter((student) => {
    return student.grade > 60;
})

console.log(passingStudents)

// [{name: "f", grade: 70}, {name: "g", grade: 80}, {name: "h", grade: 100}]
