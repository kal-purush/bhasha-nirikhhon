const prompt = require("prompt-sync")({ sigint: true });
let classSize = prompt("what is maximum number of students in class ? ");

const students = [
  { name: "Jean-Luc Garza", score: 24 },
  { name: "Teddy Munoz", score: 79 },
  { name: "Georgia Ali", score: 17 },
  { name: "Vicky Calhoun", score: 8 },
  { name: "Awais Weaver", score: 65 },
  { name: "Athena Kline", score: 52 },
  { name: "Zacharia Whitaker", score: 38 },
  { name: "Clarice Davenport", score: 99 },
  { name: "Viktoria Flynn", score: 84 },
  { name: "Ianis Crossley", score: 20 },
  { name: "Johnnie Owens", score: 74 },
  { name: "Emily-Rose Erickson", score: 33 },
  { name: "Adeel Nieves", score: 100 },
  { name: "Dustin Villegas", score: 98 },
  { name: "Maxine Hughes", score: 65 },
  { name: "Bilaal Harding", score: 79 },
  { name: "Maddie Ventura", score: 71 },
  { name: "Leroy Rees", score: 44 },
  { name: "Wanda Frank", score: 73 },
  { name: "Margaux Herbert", score: 80 },
  { name: "Ali Rios", score: 70 },
  { name: "Nigel Santiago", score: 25 },
  { name: "Markus Greene", score: 78 },
  { name: "Harlan Parrish", score: 97 },
  { name: "Baran Davidson", score: 43 },
  { name: "Seth Rodriguezh", score: 67 },
  { name: "Diego Mayer", score: 100 },
];

class hashTable {
  constructor(classSize) {
    this.classSize = classSize;
    this.classes = { A: [], B: [], C: [], D: [], other: [] };
  }

  hash = (student) => {
    {
      if (student.score >= 90) {
        return "A";
      }

      if (student.score < 90 && student.score >= 80) {
        return "B";
      }
      if (student.score < 80 && student.score >= 70) {
        return "C";
      }

      if (student.score < 70 && student.score >= 60) {
        return "D";
      }
      if (student.score < 60) {
        return "other";
      }
    }
  };

  insert = (student) => {
    let y = this.hash(student);

    if (this.classes[y].length < this.classSize) this.classes[y].push(student);
  };
}

let x = new hashTable(classSize);

let s = students.forEach((student) => {
  x.insert(student);
});

console.log(x.classes);