const petsData = [
    {
        petName: "Stella",

        age: 7,

        weightInKilos: 24,

        breed: "Dalmation"
    },

    {
        petName: "Cody",

        age: 8,

        weightInKilos: 22,

        breed: "Corgi"
    },

    {
        petName: "Mango",

        age: 2,

        weightInKilos: 11,

        breed: "Persian"
    },

    {
        petName: "Lucy",

        age: 4,

        weightInKilos: 35,

        breed: "Ball Python"
    },

    {
        petName: "Buhmie",

        age: 1,

        weightInKilos: 28,

        breed: "Bull-dog"
    },  
];


   // To know how many values we have in Pets Data. 
console.log(petsData.length);
// console.log(petsData.length) is 5, so the number shloud be btween 0 and 4. 
let userInput=prompt("Please enter a number between  0 and 4");
if(userInput<=4){
let gatherInfo = `${petsData[userInput].petName} the cat is ${petsData[userInput].age} years old. his cat weighs ${petsData[userInput].weightInKilos} kilos and is a ${petsData[userInput].breed} breed`;
console.log(gatherInfo);
let outputInfo = document.querySelector(".petsData");
outputInfo.innerHTML=`${gatherInfo}`; 
}
else { alert("Please select your number btween 0 and 4.");
}


