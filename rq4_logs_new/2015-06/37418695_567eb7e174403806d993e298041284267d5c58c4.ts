// variables types

// # 1 boolean
var isItSummer : boolean = true;

// # 2 string
var webAddres : string = 'www.technoserves.com';

// # 3 number
var noOfMonths : number = 12;
var noOfDays : number = 365.25;

// # 4 array
var friends : string[] = ["Muhammad Hashim", "Shazim"];

// # 5
// an enum is a way of giving more friendly names to sets of numeric values.
 
enum RainbowColor {red, orange, yellow, green,  blue, indigo, violet};
var myFav_rainbowColor : RainbowColor = RainbowColor.green;

// this statement is also true 
// myFav_rainbowColor = 12;

// Enum itself represent by number.
// so statement in if's condition is like 3 == 3
// because myFav_rainbowColor currently store 3 and RainbowColor.green represents 3.

if (myFav_rainbowColor == RainbowColor.green) {
	
	//We can also get the value of the enum
	console.log('you are Sheryar and your favourite color in rainbow is ' + RainbowColor[myFav_rainbowColor]);
	
	// console.log('enum has number representation : ' + RainbowColor.green);
	// console.log('Name of the value of enum : ' + RainbowColor[3]);
} else {
	console.log('you are not sheryar');
}

// # 6
// The variable of type 'any' can store all above 5 types and even functions, object ... .
// it`s type is check at the compile time.

var dynamicVar : any;
dynamicVar = 1; dynamicVar='string'; dynamicVar=true; dynamicVar = friends; dynamicVar=RainbowColor;

dynamicVar = function(){
	//now dynamicVar hold an object
	dynamicVar={};
	
}

// array of any time would behave as an list. Like list is an array that has multiple type values.
var mixDataList : any[] = [1,'string',true,friends,RainbowColor,dynamicVar];

// # 7
// the variable void type can not store a value
var restrict : void;
var nothing : number;
nothing = null;

function distinguish():any{
	if (typeof(restrict)  ==  typeof(nothing)) {
		console.log('You did some mistake.');
	}else{
		console.log('both are different.');
		console.log('restrict type : '+typeof(restrict) );
		console.log('nothing type : '+typeof(nothing) );
	}
}
distinguish();