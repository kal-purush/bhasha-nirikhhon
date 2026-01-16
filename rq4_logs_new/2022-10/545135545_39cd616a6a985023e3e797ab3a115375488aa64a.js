// [Lab] Geometry
// Opens: Wednesday, 9 November 2022, 12:00 AM

// 96. Introducing Perimeter: The theatre has 4 chairs in a row. There are 5 rows. Using 
//     rows as your unit of measurement, what is the perimeter?
let rows = 4;
let perimeter = rows + 1 + rows + 1;
console.log(`96 - The perimeter is ${perimeter}r!`);

// 97. Introducing Area: The theatre has 4 chairs in a row. There are 5 rows. How many 
//     chairs are there in total?
let rowLength = 4;
let numOfRows = 5;
let totalChairs = rowLength * numOfRows;
console.log(`97 - There are ${totalChairs} chairs in total!`);

// 98. Introducing Volume: Aaron wants to know how much candy his container can hold. The 
//     container is 20 centimetres tall, 10 centimetres long and 10 centimetres wide. What 
//     is the container’s volume?
let heightOfContainer = 20;
let lengthOfContainer = 10;
let widthOfContainer = 10;
let volumeOfContainer = heightOfContainer * lengthOfContainer * widthOfContainer;
console.log(`98 - Aaron's container has a total volume of ${volumeOfContainer}cc!`);

// 100. Finding the Perimeter of 2D Shapes: Mitchell wrote his homework questions on a 
//      piece of square paper. Each side of the paper is 8 centimetres. What is the 
//      perimeter?
let paperSide = 8;
let paperPerimeter = paperSide * 4;
console.log(`100 - The perimeter of Mitchell's homework paper is ${paperPerimeter}cm!`)

// 101. Determining the Area of 2D Shapes: A single trading card is 9 centimetres long by 
//      6 centimetres wide. What is its area?
let lengthOfCard = 9;
let widthOfCard = 6;
let areaOfCard = lengthOfCard * widthOfCard;
console.log(`101 - The area of the trading card is ${areaOfCard}cm²`)

// 103. Determining the Surface Area of 3D Shapes: What is the surface area of a cube that 
//      has a width of 2cm, height of 2 cm and length of 2 cm?
let widthOfCube = 2;
let heightOfCube = 2;
let lengthOfCube = 2;
let surfaceAreaOfCube = 6 * widthOfCube ** 2;
console.log(`103 - The surface area of the cube is ${surfaceAreaOfCube}cm²`)

// 104. Determining the Volume of 3D Shapes: Aaron’s candy container is 20 centimetres 
//      tall, 10 centimetres long and 10 centimetres wide. Bruce’s container is 25 
//      centimetres tall, 9 centimetres long and 9 centimetres wide. Find the volume of 
//      each container. Based on volume, whose container can hold more candy?
let aaronContainerHeight = 20;
let aaronContainerLength = 10;
let aaronContainerWidth = 10;
let bruceContainerHeight = 25;
let bruceContainerLength = 9;
let bruceContainerWidth = 9;
let aaronContainerVolume = aaronContainerHeight * aaronContainerLength * aaronContainerWidth;
let bruceContainerVolume = bruceContainerHeight * bruceContainerLength * bruceContainerWidth;
let containerVolumeDifference = (aaronContainerVolume < bruceContainerVolume) ? "Bruce" : 'Aaron';
console.log(`104 - Aaron\'s container is ${aaronContainerVolume}cc and Bruce\'s container is ${bruceContainerVolume}cc.\n      So ${containerVolumeDifference}\'s container is bigger!`);

// 109. Finding the Perimeter of Triangles: Luigi built a tent in the shape of an 
//      equilateral triangle. The perimeter is 21 metres. What is the length of each of 
//      the tent’s sides?
let equilateralTentPerimeter = 21;
let lengthOfTentSides = equilateralTentPerimeter / 3;
console.log(`109 - Each side of Luigi's tent has a length of ${lengthOfTentSides}m!`);

// 110. Determining the Area of Triangles: What is the area of a triangle with a base of 
//      2 units and a height of 3 units?
let triangleBase = 2;
let triangleHeight = 3;
let triangleArea = triangleBase * triangleHeight * 0.5;
console.log(`110 - The area of the triangle is ${triangleArea}u²`);

// 111. Applying Pythagorean Theorem: A right triangle has one non-hypotenuse side length 
//      of 3 inches and the hypotenuse measures 5 inches. What is the length of the other 
//      non-hypotenuse side?
let rightTriangleSideA = 3;
let rightTriangleSideC = 5;
let rightTriangleSideB = Math.sqrt(rightTriangleSideC**2 - rightTriangleSideA**2);
console.log(`111 - The other non-hypotenuse side is ${rightTriangleSideB}\" long!`);

// 112. Finding a Circle’s Diameter: Jasmin bought a new round backpack. Its area is 370 
//      square centimetres. What is the round backpack’s diameter?
let backpackArea = 370;
let backpackDiameter = 2 * Math.sqrt(backpackArea / Math.PI)
backpackDiameter = Math.round(backpackDiameter * 10) / 10;
console.log(`112 - The diameter of Jasmin\'s 2D backpack is ${backpackDiameter}cm!`);

// 113. Finding a Circle’s Area: Captain America’s circular shield has a diameter of 76.2 
//      centimetres. What is the area of his shield?
let shieldDiameter = 76.2;
let shieldArea = Math.round((Math.PI * (shieldDiameter / 2) ** 2) * 100) / 100;
console.log(`113 - The shield\'s area is ${shieldArea}cm²!`);

// 114. Finding a Circle’s Radius: Skylar lives on a farm, where his dad keeps a circular 
//      corn maze. The corn maze has a diameter of 2 kilometres. What is the maze’s radius?
let mazeDiameter = 2;
let mazeRadius = mazeDiameter / 2;
console.log(`114 - The radius of Skylar\'s corn maze is ${mazeRadius}km!`);


