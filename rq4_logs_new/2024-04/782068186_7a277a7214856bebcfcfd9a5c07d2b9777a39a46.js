// TODO: Include packages needed for this application
const inquirer = require('inquirer');
const fs = require('fs');

// TODO: Create an array of questions for user input
const questions = [
    {
        type: 'input',
        message: 'What is your project title?',
        name: 'title',
    },
    {
        type: 'input',
        message: 'Please provide a description of your project',
        name: 'description',
    },
    {
        type: 'input',
        message: 'List installation instruction (if any)',
        name: 'installation',
    },
    {
        type: 'input',
        message: 'Please detail usage information',
        name: 'usage',
    },
    {
        type: 'input',
        message: 'List details for user contribution',
        name: 'contribution',
    },
    {
        type: 'input',
        message: 'Please detail the credit for the project',
        name: 'credit',
    },
    {
        type: 'input',
        message: 'List all testing information',
        name: 'test',
    },
    {
        type: 'list',
        message: 'List all testing information',
        choices: ['No License', 'MIT License', 'Other License'],
        name: 'license',
    },
    {
        type: 'input',
        message: 'Please type your GitHub username',
        name: 'username',
    },
    {
        type: 'input',
        message: 'Please enter your email address',
        name: 'email',
    },
];

// TODO: Create a function to write README file
function writeToFile(fileName, data) {
    fs.writeFile(fileName, data, (err) =>
        err ? console.error(err) : console.log('HTML appended!'))
}

// TODO: Create a function to initialize app
function init() {
    inquirer
        .prompt(questions)
        .then((response) => {
            console.log(response);
            const { title, description, installation, usage, contribution, credit, test, license, username, email } = response;
            const readMeText = `
            # ${title}

            ## Description

            ${description}

            ## Table of Contents

            - [Installation](#installation)
            - [Usage](#usage)
            - [Credits](#credits)
            - [License](#license)

            ## Installation

            ${installation}

            ## Usage

            ${usage}

            ## Credits

            ${credit}

            ## License

            ${license}

            ## Badges

            ![badmath](https://img.shields.io/github/languages/top/lernantino/badmath)

            Badges aren't necessary, per se, but they demonstrate street cred. Badges let other developers know that you know what you're doing. Check out the badges hosted by [shields.io](https://shields.io/). You may not understand what they all represent now, but you will in time.

            ## How to Contribute

            ${contribution}

            ## Tests

            ${test}

            ## Questions

            For questions/inquiries, please direct them to:
            
            [GitHub Profile](https://github.com/${username})
            Email: ${email}

            `;
            writeToFile("newREADME.md", readMeText);
        })
}

// Function call to initialize app
init();