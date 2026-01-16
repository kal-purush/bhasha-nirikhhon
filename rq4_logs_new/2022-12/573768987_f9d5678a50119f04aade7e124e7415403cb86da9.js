//Mathematical Shapes

//1. Diagonal of a square

const squareLength = 9;

const diagLength = squareLength * Math.sqrt(2);

console.log(diagLength);

//2. Area of a triangle

const sideA = 5;
const sideB = 6;
const sideC = 7;

const s = (sideA + sideB + sideC) / 2;

const areaOfTriangle = Math.sqrt(s * ((s - sideA) * (s - sideB) * (s - sideC))); 

console.log(areaOfTriangle)

//3. Circumference and surface area of a circle

const radiusOfCircle = 4;

const circumferece = 2 * Math.PI * 4;

console.log(circumferece);






