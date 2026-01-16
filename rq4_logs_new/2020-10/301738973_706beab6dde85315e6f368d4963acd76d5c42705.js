const inquirer = require("inquirer");
const fs = require("fs");
const generateMarkdown = require("./utils/generateMarkdown");

inquirer.prompt([
    {
        message: "What is your project name?",
        name: "title",
        type: "input"
    },
    {
        message: "How would you describe your project?",
        name: "description",
        type: "input"
    },
    {
        message: "What is your Github username?",
        name: "github",
        type: "input"
    },
    {
        message: "What year was this project created?",
        name: "yearcreated",
        type: "input"
    },
    {
        message: "What is the full name of the user attaining the license?",
        name: "fullname",
        type: "input"
    },
    {
        message: "What command should be run to install dependencies?",
        name: "install",
        type: "input"
    },
    {
        message: "What is the purpose of this project?",
        name: "usage",
        type: "input"
    },
    {
        message: "Is there anyone else who contributed to credit on this project?",
        name: "credits",
        type: "input"
    },
    {
        message: "Does the user have any other instructions to help understand the project more?",
        name: "instructions",
        type: "input"
    },{
        message: "Do user have any links to support the project? ",
        name: "source",
        type: "input"
    },{
        message: "Does user have any badges for this project?",
        name: "badge",
        type: "input"
    },
    {
        message: "What kind of license should your project have?",
        name: "license",
        type: "input"
    },
    {
        message: "What is your email address?",
        name: "email",
        type: "input"
    },
    
])
    .then(function (answers) {
        console.log(answers);

        const generatedFile = generateMarkdown(answers)

        fs.writeFile("README.md", generatedFile, (err) => {
            if (err) {
                return console.log(err);
            }
            console.log("File written!");
        });
    })