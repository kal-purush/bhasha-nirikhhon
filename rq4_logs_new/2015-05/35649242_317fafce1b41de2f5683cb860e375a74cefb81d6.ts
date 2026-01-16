
// There are the simple basic types that everything is composed of:

//---------------------
// BOOLEAN
//---------------------

// Only allows true or false.
let isItTrue:boolean = true;  

// Does not allow things that eval to true or false...
let canIHold0:boolean = 0;
let canIHold0:boolean = 1;
let canIHoldEmpty:boolean = '';


//---------------------
// NUMBER
//---------------------
let age:number = 123;
let infinity:number = Number.POSITIVE_INFINITY;


//---------------------
// STRING
//---------------------
let myName:string = 'Jimmy';


//---------------------
// ARRAY
//---------------------
let grades:number[] = [1,2,3,4];
let ages:Array<number> = [1,2,3,44];
let badAges:Array<number> = [1,3,2,'4'];

let myList:any[] = [1,'',{'hi':23}];

//---------------------
// ENUMS
//---------------------
// By default, enums are 0 indexed
enum LETTER_GRADES {A, B, C, D, F};
let myGrade:LETTER_GRADES = LETTER_GRADES.A; // = 0

// Or, you can set the startin value
enum YEARS {FIRST = 2011, SECOND, THIRD};
let myYear:YEARS = YEARS.SECOND;  // = 2012;

// We can even go backwards.
let whatYear:string = YEARS[2012]; // = 'SECOND'


//---------------------
// ANY
//---------------------

//---------------------
// VOID
//---------------------