// Structural typing
interface INamed {
    name: string;
}

class Citizen {
    public readonly name: string;
}

let c: INamed;
c = new Citizen();

let c2: INamed;
let y = { name: "foo", age: 25 };

c2 = y;

function greet(n: INamed) {
    console.log(n.name);
}

greet(y);

// comparing functions
let x1 = (a: number) => 0;
let y1 = (b: number, s: string) => 0;

y1 = x1; // OK
// x1 = y1; // Error

let x2 = () => ({name: "Alice"});
let y2 = () => ({name: "Alice", location: "Seattle"});

x2 = y2; // OK
// y2 = x2; // Error because x()
// lacks a location property

// Bivariance

interface INoun {
    name: string;
}
interface ICity extends INoun {
    name: string;
}

interface INewYork extends ICity {
    name: string;
}

// allows more specialized and less
// specialized types to be passed
// Sub-types of ICity and Super-types of
// of ICity are compatible.
function bivariance(o: ICity) {
    console.log(o.name);
}

let noun: INoun = { name: "Noun" };
let city: ICity = { name: "City" };
let newYork: INewYork = { name: "NewYork" };

bivariance(noun);
bivariance(city);
bivariance(newYork);