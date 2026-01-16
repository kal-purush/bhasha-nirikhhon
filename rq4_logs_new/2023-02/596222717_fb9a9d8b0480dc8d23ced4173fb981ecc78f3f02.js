//- Знайти та вивести довижину настипних стрінгових значень
//'hello world', 'lorem ipsum', 'javascript is cool'
let str1='hello world'
let str2='lorem ipsum'
let str3='javascript is cool'
console.log(str1.length);
console.log(str2.length);
console.log(str3.length);

//- Перевести до великого регістру наступні стрінгові значення
//'hello world', 'lorem ipsum', 'javascript is cool'
//- Перевести до нижнього регістру настипні стрінгові значення
//'HELLO WORLD', 'LOREM IPSUM', 'JAVASCRIPT IS COOL'
console.log(str1.toUpperCase());
console.log(str2.toUpperCase());
console.log(str3.toUpperCase());
console.log(str1.toLowerCase());
console.log(str2.toLowerCase());
console.log(str3.toLowerCase());
//- Є "брудна" стрінга let str = ' dirty string   ' . Почистити її від зайвих пробілів.
let str4 = ' dirty string   '
let str5=str4.trim()
console.log(str5.length)
//- Напишіть функцію stringToarray(str), яка перетворює рядок на масив слів.
 // let str = 'Ревуть воли як ясла повні';
//let arr = stringToarray(str); ['Ревуть', 'воли', 'як', 'ясла', 'повні']
let str6 = 'Ревуть воли як ясла повні'
console.log(str6.split(' '))

//- є масив чисел [10,8,-7,55,987,-1011,0,1050,0] . за допомоги map  перетворити всі об'єкти в масиві на стрінгові.
 let arr1= [10,8,-7,55,987,-1011,0,1050,0]
for (const number of arr1) {

}
let arr2= arr1.map(number=>''+number)
console.log(arr2)
console.log(typeof arr2[0])
//- створити функцію sortNums(direction), яка прймає масив чисел, та сортує його від більшого до меньшого, або навпаки в залежності від значення аргументу direction.
  const nums = [11,21,3];
//sortNums(nums,'ascending') // [3,11,21]
//sortNums(nums,'descending') // [21,11,3]
let sortNums=(arr,direction)=>{
  switch (direction){
    case 'ascending':return arr.sort((a,b) => a-b);
    case 'descending':return arr.sort((a,b) => b-a);
  }
}
console.log(sortNums(nums,'ascending'))
console.log(sortNums(nums,'descending'))
//==========================
//- є масив
let coursesAndDurationArray = [
  {title: 'JavaScript Complex', monthDuration: 5},
  {title: 'Java Complex', monthDuration: 6},
  {title: 'Python Complex', monthDuration: 6},
  {title: 'QA Complex', monthDuration: 4},
  {title: 'FullStack', monthDuration: 7},
  {title: 'Frontend', monthDuration: 4}
];
//-- відсортувати його за спаданням за monthDuration
console.log(coursesAndDurationArray.sort((a, b) => b.monthDuration - a.monthDuration));
//-- відфільтрувати , залишивши тільки курси з тривалістю більше 5 місяців
console.log(coursesAndDurationArray.filter((item) => item.monthDuration > 5));
//-- за допомоги map перетворити кожен елемент на наступний тип {id,title,monthDuration}
console.log(coursesAndDurationArray.map((item, index) => ({...item, id: index + 1})));
//=========================
  //описати колоду карт (від 6 до туза без джокерів)
let cards=[
  {cardSuit:'spade',value:'6',color:'red',},
  {cardSuit:'spade',value:'7',color:'red',},
  {cardSuit:'spade',value:'8',color:'red',},
  {cardSuit:'spade',value:'9',color:'red',},
  {cardSuit:'spade',value:'10',color:'red',},
  {cardSuit:'spade',value:'ace',color:'red',},
  {cardSuit:'spade',value:'jack',color:'red',},
  {cardSuit:'spade',value:'queen',color:'red',},
  {cardSuit:'spade',value:'king',color:'red',},
  {cardSuit:'spade',value:'jocker',color:'red',},
  {cardSuit:'heart',value:'6',color:'red',},
  {cardSuit:'heart',value:'7',color:'red',},
  {cardSuit:'heart',value:'8',color:'red',},
  {cardSuit:'heart',value:'9',color:'red',},
  {cardSuit:'heart',value:'10',color:'red',},
  {cardSuit:'heart',value:'ace',color:'red',},
  {cardSuit:'heart',value:'jack',color:'red',},
  {cardSuit:'heart',value:'queen',color:'red',},
  {cardSuit:'heart',value:'king',color:'red',},
  {cardSuit:'heart',value:'jocker',color:'red',},
  {cardSuit: 'diamond',value: '6',color: 'black'},
  {cardSuit: 'diamond',value: '7',color: 'black'},
  {cardSuit: 'diamond',value: '8',color: 'black'},
  {cardSuit: 'diamond',value: '9',color: 'black'},
  {cardSuit: 'diamond',value: '10',color: 'black'},
  {cardSuit: 'diamond',value: 'ace',color: 'black'},
  {cardSuit: 'diamond',value: 'jack',color: 'black'},
  {cardSuit: 'diamond',value: 'queen',color: 'black'},
  {cardSuit: 'diamond',value: 'king',color: 'black'},
  {cardSuit: 'diamond',value: 'jocker',color: 'black'},
  {cardSuit: 'clubs',value: '6',color: 'black'},
  {cardSuit: 'clubs',value: '7',color: 'black'},
  {cardSuit: 'clubs',value: '8',color: 'black'},
  {cardSuit: 'clubs',value: '9',color: 'black'},
  {cardSuit: 'clubs',value: '10',color: 'black'},
  {cardSuit: 'clubs',value: 'ace',color: 'black'},
  {cardSuit: 'clubs',value: 'jack',color: 'black'},
  {cardSuit: 'clubs',value: 'queen',color: 'black'},
  {cardSuit: 'clubs',value: 'king',color: 'black'},
  {cardSuit: 'clubs',value: 'jocker',color: 'black'},
]
//- знайти піковий туз
console.log(cards.find((cards) => cards.cardSuit ==='spade' && cards.value === 'ace'));
//- всі шістки
console.log(cards.filter((cards) => cards.value === '6'));
//- всі червоні карти
console.log(cards.filter((cards) => cards.color === 'red'));
//- всі буби
console.log(cards.filter((cards) => cards.cardSuit === 'diamond'));
//- всі трефи від 9 та більше
console.log(cards.filter((cards) => cards.cardSuit === 'clubs' &&  ['9','10','ace','jack','queen','king','jocker'].includes(cards.value)));

//{
 // cardSuit: '', // 'spade', 'diamond','heart', 'clubs'
  //  value: '', // '6'-'10', 'ace','jack','queen','king','joker'
  //color:'', // 'red','black'
//}

//=========================

  //Взяти описану колоду карт, та за допомоги reduce упакувати всі карти по "мастях" в об'єкт
//{
  //spades:[],
  //diamonds:[],
  //hearts:[],
  //clubs:[]
//}
let suit=cards.reduce((acc, current)=>{
  acc[current.cardSuit].push(current);
  return acc
},{spade:[],clubs:[],diamond:[],heart:[]})
console.log(suit)
//=========================
  //взяти з arrays.js (який лежить в папці 2023 plan) масив coursesArray
let coursesArray = [
  {
    title: 'JavaScript Complex',
    monthDuration: 5,
    hourDuration: 909,
    modules: ['html', 'css', 'js', 'mysql', 'mongodb', 'react', 'angular', 'aws', 'docker', 'git', 'node.js']
  },
  {
    title: 'Java Complex',
    monthDuration: 6,
    hourDuration: 909,
    modules: ['html',
      'css',
      'js',
      'mysql',
      'mongodb',
      'angular',
      'aws',
      'docker',
      'git',
      'java core',
      'java advanced']
  },
  {
    title: 'Python Complex',
    monthDuration: 6,
    hourDuration: 909,
    modules: ['html',
      'css',
      'js',
      'mysql',
      'mongodb',
      'angular',
      'aws',
      'docker',
      'python core',
      'python advanced']
  },
  {
    title: 'QA Complex',
    monthDuration: 4,
    hourDuration: 909,
    modules: ['html', 'css', 'js', 'mysql', 'mongodb', 'git', 'QA/QC']
  },
  {
    title: 'FullStack',
    monthDuration: 7,
    hourDuration: 909,
    modules: ['html',
      'css',
      'js',
      'mysql',
      'mongodb',
      'react',
      'angular',
      'aws',
      'docker',
      'git',
      'node.js',
      'python',
      'java']
  },
  {
    title: 'Frontend',
    monthDuration: 4,
    hourDuration: 909,
    modules: ['html', 'css', 'js', 'mysql', 'mongodb', 'react', 'angular', 'aws', 'docker', 'git', 'sass']
  }
];
//--написати пошук всіх об'єктів, в який в modules є sass
console.log(coursesArray.filter((item) => item.modules.includes('sass')));
//--написати пошук всіх об'єктів, в який в modules є docker
console.log(coursesArray.filter((item) => item.modules.includes('docker')));