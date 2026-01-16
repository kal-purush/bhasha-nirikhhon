// TODO: Include packages needed for this application
const inquirer = require('inquirer');
const fs = require ("fs");
const README = ({title, description, installation, usage, credits, license, features}) =>
`#${title}
##Description
${description}
##Installation
${installation}
##Usage
${usage}
##Credits
${credits}
##License
${license}
##Features
${features}`
// TODO: Create an array of questions for user input
const questions = [{
    type: 'input',
    message: 'what is the title of you README?',
    name: 'title',
},
{
    type: 'input',
    message: 'a description of your project(motivation? what problem does it solve? what did you learn?)',
    name: 'description',
},
{
    type: 'input',
    message: 'what is the installation process?',
    name: 'installation',
},
{
    type: 'input',
    message: 'how do you use the application?',
    name: 'usage',
},
{
    type: 'input',
    message: 'what resources did you use and who helped you?',
    name: 'credits',
},
{
    type: 'input',
    message: 'what license did you use?',
    name: 'license',
},
{
    type: 'input',
    message: 'what special features does it have?',
    name: 'features',
}]
inquirer
    .prompt(questions)
// .then((answers) => {
//     const htmlContent = html(answers)
//     fs.appendFile("index.html", JSON.stringify(answers), error =>{
//     !error ? console.log('success!')
//     : console.log('an error occured')
//     })
// })

// TODO: Create a function to write README file
function writeToFile(fileName, data) {}

// TODO: Create a function to initialize app
function init() {}

// Function call to initialize app
init();