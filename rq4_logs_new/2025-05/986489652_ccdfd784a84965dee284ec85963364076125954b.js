const müştərilər = [
  { ad: "Əli", şəhər: "Bakı", yaş: 25 },
  { ad: "Aytən", şəhər: "Gəncə", yaş: 30 },
  { ad: "Murad", şəhər: "Bakı", yaş: 28 },
  { ad: "Lalə", şəhər: "Sumqayıt", yaş: 22 },
];

console.log(müştərilər.filter((cus) => cus.şəhər === "Bakı"));

const adlar = ["əli", "aytən", "murad", "lalə"];
console.log(adlar.map((ad) => ad.toUpperCase()));

const qiymətlər = [29.99, 10.5, 5.75, 16.3];

console.log(qiymətlər.reduce((acc, num) => acc + num, 0).toFixed(2));

let numbers = [1, 5, 10, 15];

console.log(numbers.find((num) => num > 8)); // 10
console.log(numbers.findIndex((num) => num > 8)); // 2

const fruits = ["uzum", "nar", "alma", "armud", "banan", "kivi"];

console.log(fruits.sort());

let nums = [
  21, 11, 1, 13, 14, 23, 26, 16, 32, 45, 34, 16, 18, 24, 42, 5, 3, 2, 7, 4, 4,
];

console.log(nums.sort((a, b) => a - b));

let arr = [1, [2, 3], [4, [5, 6]]]; // => [1, 2, 3, 4 ,5, 6]

let newArr = [...arr];
console.log(arr.flat(2));


const arr22 = [1, 4, 2, , 6, 7, 12, 15, 17, 14, 11];

// ==== for ====

// const getSpecNum = (num) => {
//     let specNum = [];

//     for (let i = 0; i < num.length; i++) {
//         if (num[i] % 3 === 0) {
//             specNum.push(num[i]);
//         }
//     }

//     return specNum;
// };

// =================
// ==== forEach ====
// =================

// const getSpecNum = (num) => {
//   let specNum = [];

//   function chechNum(number) {
//     if (number % 3 === 0) {
//       specNum.push(number);
//     }
//   }

//   num.forEach(chechNum);

//   return specNum;
// };

// const getSpecNum = (num) => {
//   let specNum = [];

//   num.forEach(function chechNum(number) {
//     if (number % 3 === 0) {
//       specNum.push(number);
//     }
//   });

//   return specNum;
// };

// const getSpecNum = (num) => {
//   let specNum = [];

//   num.forEach((number) => (number % 3 === 0 ? specNum.push(number) : ""));

//   return specNum;
// };

// =================
//  ==== filter ====
// =================

// const getSpecNum = (num) => {
//   let specNum = [];

//   const result = num.filter((number) => (number % 3 === 0 ? true : false));

//   return result;
// };

// const getSpecNum = (num) => {
//   return num.filter((number) => (number % 3 === 0));
// };
const getSpecNum = (num) => num.filter((number) => number % 3 === 0);
getSpecNum(arr);

arr.filter((num) => num % 3 === 0);

// ==============================
// ===== sort & unsort array ====
// ==============================

const sortArray = (num) => {
  //   const sortedArray = num.sort(); // мутирующий нет необходимости присваивать к переменной
  //    num.sort(); // сортирует по ASCII table

  //   function compare(a, b) {
  //     if (a > b) {
  //       return 1;
  //     }
  //     return -1;
  //   }

  //   num.sort(compare);

  num.sort((a, b) => a > b);
  num.sort((a, b) => a - b);

  //   ters
  num.sort((a, b) => b - a);

  return num; // orijinal massivi qaytarır
};

const sortArray1 = (num) => {
  const copy = num.slice(); // massivi kopyaliyir

  copy.sort((a, b) => a > b);
  copy.sort((a, b) => a - b);

  //   ters
  copy.sort((a, b) => b - a);

  return copy; // yeni massiv qaytarir
};

const sortArray2 = (num) => {
  //   const copy = num.slice(); // ara dəyişənlərdən istifadə edə bilməzsiniz

  num.slice().sort((a, b) => a > b);
  num.slice().sort((a, b) => a - b);

  //   в обратном порядке
  num.slice().sort((a, b) => b - a);

  return num; // yeni massiv qaytarir
};

// ===========================================
// ==== JavaScript-də çoxölçülü massivlər ====
// ===========================================

// const arrM = [1, [2, 3, 4], 5, [6, 7]]  => [1,2,3,4,5,6,7];
// use concat
const arrM = [1, [2, 3, 4], 5, [6, 7]];
// const flatArray = (arr) => {
//     const flatArray = arr.reduce((acc, elem) => {
//         return acc.concat(elem)
//     }, [])
//     return flatArray
// };
// flatArray(arrM)

// daha optimal
const flatArray = (arr) => {
  return arr.reduce((acc, elem) => acc.concat(elem), []);
};
flatArray(arrM);

// [1, 2, 3, 4] => 10;
const redArr = [1, 2, 3, 4];

redArr.reduce(function (acc, num) {
  return acc + num;
});
redArr.reduce((acc, num) => acc + num);

// ==== flat ====

const flatArr = (arr) => {
  return arr.flat();
};
flatArr(arrM);

// failed Studets

const allStudents = ["Anna", "Tom", "Bob", "Kate"];
const failedStudents = ["Tom", "Bob"];

// use indexOf
const getPassedStudents = (allStudents, failedStudents) => {
  const passedStudents = allStudents.filter(
    (name) => failedStudents.indexOf(name) === -1
  );
  return passedStudents;
};
getPassedStudents(allStudents, failedStudents);

const getPassedStudents2 = (allStudents, failedStudents) => {
  const passedStudents = allStudents.filter((name) =>
    failedStudents.includes(name)
  );
  return passedStudents;
};
getPassedStudents2(allStudents, failedStudents);

// use map to add message body
const getPassedStudentsMessage = (allStudents, failedStudents) => {
  const passedStudents = allStudents.filter(
    (name) => !failedStudents.includes(name)
  );
  const message = passedStudents.map((name) => `Good job ${name}`);
  return message;
};
getPassedStudentsMessage(allStudents, failedStudents);

const getPassedStudentsMessageOptimize = (allStudents, failedStudents) =>
  allStudents
    .filter((name) => !failedStudents.includes(name))
    .map((name) => `Good job ${name}`);

getPassedStudentsMessageOptimize(allStudents, failedStudents);
