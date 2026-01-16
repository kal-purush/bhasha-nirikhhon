// simple types
var isDone: boolean = false;
var heigth: number = 6;
var name: string = 'Rinat';
var test: number;
test = 1;
var check = 1;

// Array types can be writte in one of two ways:
var list: number[] = [1, 2, 3];
var list2: Array<number> = [1, 2, 3];

// Enum
enum Color {Red = 1, Green = 2, Blue = 3};
var c: Color = Color.Red;
console.log(Color[2]);

// Any type - like js
var notSure: any = 4;
notSure = 'maybe a string instead';
notSure = false;

var listMixTypes: any[] = [1, true, 'free'];

// Void
function warnUser(): void {
    console.log('This is my warning message');
}