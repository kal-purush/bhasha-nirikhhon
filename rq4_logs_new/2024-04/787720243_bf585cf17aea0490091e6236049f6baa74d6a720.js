#! /usr/bin/env node
// the above line is called shebang,it tells the OS to run it with nodejs
// import the inquirer module which is a command line interface for nodejs
import inquirer from "inquirer";
const answers = await inquirer.prompt([{
        name: "Sentence",
        type: "input",
        message: "Enter your sentence to count the word"
    }]);
const words = answers.Sentence.trim().split(" ");
console.log(words);
console.log(`Your sentence word count is ${words.length}`);