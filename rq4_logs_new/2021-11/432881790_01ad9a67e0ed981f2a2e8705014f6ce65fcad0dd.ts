// 1. Setting types
var myString: string;
// I can assign myString like this:
myString = "Bee stinger";
// Why is there a problem with this? What can I do to fix this?
myString = "9";
// The variable myString most be a type string


// 2. Setting the types for function parameters
function sayHello(name: string){
    return `Hello, ${name}!`;
}
 // This is working great:
console.log(sayHello("Kermit"));
 // Why isn't this working? I want it to return "Hello, 9!"
console.log(sayHello("9"));
 //sayHello only receives strings, so 9 most be change into a string


// 3. Optional parameters
function fullName(firstName: string, lastName: string, middleName: string){
    let fullName = `${firstName} ${middleName} ${lastName}`;
    return fullName;
}
 // This works:
console.log(fullName("Mary", "Moore", "Tyler"));
 // What do I do if someone doesn't have a middle name?
console.log(fullName("Jimbo", "Jones", "Karen"));
 //The function fullName is specting three parameters and we are sending only two, to fix it we most add the third one
 //Or we can add a ? to the value of ==> middleName?: string, to not see the error, but we will get an undefined in the console.log for that value


// 4. Interfaces and function parameters
interface Student {
    firstName: string;
    lastName: string;
    belts: number;
}
function graduate(ninja: Student){
    return `Congratulations, ${ninja.firstName} ${ninja.lastName}, you earned ${ninja.belts} belts!`;
}
const christine = {
    firstName: "Christine",
    lastName: "Yang",
    belts: 2
}
const jay = {
    firstName: "Jay",
    lastName: "Patel",
    belts: 4
}
 // This seems to work fine:
console.log(graduate(christine));
 // This one has problems:
console.log(graduate(jay));
 //const belt variable inside the object jay is missing an s at the end, it should be belts. 
 //if an object is const, we can't change the object's structure (such as adding, or replacing keys), but we can change the object's values.
 //And beacause we have to use the same properties of the interface Student, when using the function graduete, me most change belt to belts in const jay.


// 5. Classes and function parameters
class Ninja {
    fullName: string;
    constructor( public firstName: string, public lastName: string){
        this.fullName = `${firstName} ${lastName}`;
    }
    debug(){
        console.log("Console.log() is my friend.")
    }
}
 // This is not making an instance of Ninja, for some reason:
 //const shane = Ninja();
 const shane = new Ninja("Shane", "Wasabi"); //You need to add the key word new to create a new instance and include the exact number of properties in the object Ninja
 // Since I'm having trouble making an instance of Ninja, I decided to do this:
 // The const turing is not a way to make a instance, you have to do this: const turing = new Ninja("Alan", "Turing");
/*const turing = {
    fullName: "Alan Turing",
    firstName: "Alan",
    lastName: "Turing"
}*/
 // Now I'll make a study function, which is a lot like our graduate function from above:
function study(programmer: Ninja){
    return `Ready to whiteboard an algorithm, ${programmer.fullName}?` 
}
const turing = new Ninja("Alan", "Turing");
console.log(study(turing)); 
console.log(study(shane)); 


// 6. Arrow functions
var increment = (x: number) => x + 1; //You need to declare what type is x
// This works great:
console.log(increment(3));
var square = (x: number) => x * x; //Get rid of the curly braces beetwen the x*x
// This is not showing me what I want:
console.log(square(4));
// This is not working:
var multiply = (x:number, y:number) => x * y; //You need to declare what type is x and y
console.log(multiply(2,5));
// Nor is this working:
var math = (x:number, y:number) => { //You need to declare what type is x and y
    let sum = x + y;
    let product = x * y;
    let difference = Math.abs(x-y);
    return [sum, product, difference];
}
console.log(multiply(2,5));

// 7. Arrow functions and 'this' 
class Elephant {
    constructor(public age: number){}
    //birthday = function(){}
    birthday = () =>{ //convert by getting rid of function and adding arrows
        this.age++;
    }
}
const babar = new Elephant(8);
setTimeout(babar.birthday, 1000)
setTimeout(function(){
    console.log(`Babar's age is ${babar.age}.`)
    }, 2000)
 // Why didn't babar's age change? Fix this by using an arrow function in the Elephant class.