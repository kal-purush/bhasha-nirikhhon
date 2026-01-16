// TODO: Include packages needed for this application
const inquirer = require('inquirer');
const MarkDown = require('./utils/ReadmeGen')
const fs = require('fs');
// const generateMarkdown = require('./utils/generateMarkdown');
// // TODO: Create an array of questions for user input
const questions = require('./utils/questions')


function runQuery() {
    return inquirer.prompt(questions)
        .then(answers => {
            const mark = MarkDown.generateReadme(answers);
            fs.writeFile('README.md', mark, (err)=>{
            err ? console.log(err) : console.log('README.md file has been generated.')
            })
            return answers;
        })
        .catch((error) => {
            console.log(error);
        })
}
runQuery()