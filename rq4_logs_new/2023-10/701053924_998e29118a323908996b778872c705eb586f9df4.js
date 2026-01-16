const inquirer= require("inquirer");
const fs= require("fs");
const { Triangle, Square, Circle, Rectangle } = require("./lib/shapes")


//makes the logo and puts it in //
function writeToFile(fileName, answers) {
  let logoString = "";
  logoString = '<svg version="1.1" width="300" height="200" xmlns="http://www.w3.org/2000/svg">';
  logoString += "<g>";
  logoString += `${answers.shape}`;

//this is the shape with the background color, based on which shape is chosen from dropdown//
let shapeChoice;
  if (answers.shape === "Triangle") {
    shapeChoice = new Triangle();
    logoString += `<polygon points="0 200, 150 0, 300 200" fill="${answers.shapeBackgroundColor}"/>`;
  } else if (answers.shape === "Square") {
    shapeChoice = new Square();
    logoString += `<rect x="50" y="0" width="200" height="200" fill="${answers.shapeBackgroundColor}"/>`;
  } else {
    shapeChoice = new Circle();
    logoString += `<circle cx="150" cy="100" r="100" fill="${answers.shapeBackgroundColor}"/>`;
  }

  //this builds the string to be written//
  logoString += `<text x="150" y="100" text-anchor="middle" font-size="40" fill="${answers.textColor}">${answers.text}</text>`;
  logoString += "</g>";
  logoString += "</svg>";
  fs.writeFile(fileName, logoString, (err) => {
    err ? console.log(err) : console.log("Generated logo.svg");
});
}

//Asks the questions.//
function promptUser() {
    inquirer
      .prompt([
        // Text 
        {
          type: "input",
          name: "text",
          message: "What text would you like you logo to contain?",
        },
        // Text color
        {
          type: "input",
          name: "textColor",
          message: "What color would you like your text to be? (Please enter color keyword OR a hexadecimal number)",
          
        },
        // Shape choice 
        {
          type: "list",
          name: "shape",
          message: "What shape would you like the logo to be?",
          choices: ["Triangle", "Square", "Circle"],
          
        },
        // Shape color 
        {
          type: "input",
          name: "shapeBackgroundColor",
          message: "What color would you like the shape to be?",
        
        },
      ])
      .then((answers) => {
        if (answers.text.length > 3) {
          console.log("No more than 3 characters, please!");
          promptUser();
        } else {
          //this writes the file to the examples folder//
          writeToFile("./examples/logo.svg", answers);
        }
      });
  }
  
  promptUser();