/*var a:Number = 23;

enum doorstate{
	open,
	close = 0,
	ajar
}
alert(doorstate[0]);*/

/*var	concatStrings	=	function(a?:	string,	b:	string = "s",	...args:	string[]){				
	return	a	+	b	+	args; 
		}
console.log(concatStrings("a",	"b",	"c")); 
console.log(concatStrings("a",	"b")); 
console.log(concatStrings("a",	"b",	"c","f","s","j"));*/

/*if (true) {
  let a = 4;
  // use a
}
else {
  let a = "string";
  // use a
}

alert(a);*/

/*let myany = Array<number>[]
myany = ['string',23,true]*/

/*let myType : any = { name: "Zia", id: 1 };
myType = { id: 2,  name: "Tom" };// can only assign a type which has the at least the same properties
myType = { id: 3,  name: "Mike", gender: false };//can add a property
myType = { name: "Mike", gender: true };*/

/*enum Color {Red, Green, Blue};//starts with 0
var c: Color = Color.Green;
console.log(c);

enum Color1 {Red = 1, Green, Blue};
var colorName: string = Color[2];
console.log(colorName);

enum Color2 {Red = 1, Green = 2, Blue = 4};//can assign values to all
var colorIndex = Color2["Blue"];
console.log(colorIndex);*/

/*var add = (a: number, b: number) => {
    return a + b;
}

var add1 = (x: number, y: number) => x + y;

var myFunction = () => { this.x = "x"; }
var e = new myFunction();
alert(e.x);*/

function myCallBack(text: string) {
    console.log("inside myCallback " + text);
}

function callingFunction(initialText: string,
    callback: (tex: string) => void)
{
    callback(initialText);
}

callingFunction("myText", myCallBack)

