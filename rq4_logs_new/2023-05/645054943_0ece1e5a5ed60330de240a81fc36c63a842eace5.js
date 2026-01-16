// Lesson 6: template literal strings, optimizing code, classes, import and export

function print(input){
    console.log(input);
}

// Template literal strings - similar to f{} strings in python, can be multi line
var biodata = {name:'Haider', uni:'Habib University', class:2024};

var greeting = `Hello ${biodata.name} from the class of ${biodata.class}! 
Welcome to ${biodata.uni}! Hope you have an exciting semester ahead!`;

print(greeting);


// Optimizing general tasks - by making them concise

// creating a object from a function using arrow functions
var biodatagenerator = (name, age, gender) => ({name, age, gender});

var biodata = biodatagenerator('Asim', 58, 'Male');
print(biodata);

// classes - for constructing a new object
// not necessary but convension to have class name with caps

class iphone{
    constructor(model, storage, color, condition, price){
        this.model = model;
        this.storage = storage;
        this.color = color;
        this.condition = condition;
        this.price = price;
    }
}

const phone = new iphone('iPhone 14 pro', 64, 'Space Black', 'Brand New', '$999');
print(phone);

// getters and setters

class Circle{
    constructor(radius){
        this._radius = radius;
    }

    get radius(){
        return this._radius;
    }

    set radius(newradius){
        if (newradius > 0){
            this._radius = newradius;
        }
        else{
            console.log("Invalid radius value. Radius must be greater than 0.");
        }
    }

    get area(){
        return Math.PI * this._radius ** 2;
    }
}

var circle = new Circle(5);
console.log(circle.radius); // Output: 5

circle.radius = 8;
console.log(circle.radius); // Output: 8

circle.radius = -3; // Invalid radius value. Radius must be greater than 0.
console.log(circle.radius); // Output: 8 (unchanged)

console.log(circle.area); // Output: 201.06192982974676


// import and export 

// have export keyword before the function/thing you want to export from that respective file 

import { capatlizestring } from "./lesson5.js"

const formattedgreeting = capatlizestring('hello! welcome to our office');
print(formattedgreeting);

// importing entire file 
import * as lesson3content from "./lesson3.js";
