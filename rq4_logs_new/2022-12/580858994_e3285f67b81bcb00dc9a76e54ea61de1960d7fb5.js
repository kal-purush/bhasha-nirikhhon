#! /usr/bin/env node
import inquirer from "inquirer";
import chalk from "chalk";
import chalkAnimation from 'chalk-animation';
const sleep = () => {
    return new Promise((res) => {
        setTimeout(res, 2000);
    });
};
async function welcome() {
    let welocmenote = chalkAnimation.rainbow("Lets Start Calculations!");
    await sleep();
    welocmenote.stop();
    console.log(chalk.green(`_____________________
|  _________________  |
| | TO           0. | |
| |_________________| |
|  ___ ___ ___   ___  |
| | 7 | 8 | 9 | | + | |
| |___|___|___| |___| |
| | 4 | 5 | 6 | | - | |
| |___|___|___| |___| |
| | 1 | 2 | 3 | | x | |
| |___|___|___| |___| |
| | . | 0 | = | | / | |
| |___|___|___| |___| |
|_____________________|`));
}
await welcome();
async function askQuestion() {
    const answers = await inquirer
        .prompt([
        {
            type: "list",
            name: "operator",
            message: "Which operation you want to perform? \n",
            choices: ["Addition", "Subtraction", "Multiplication", "Division"]
        },
        {
            type: "number",
            name: "num1",
            message: "Enter number 1: "
        },
        {
            type: "number",
            name: "num2",
            message: "Enter number 2: "
        }
    ]);
    // .then((answers)=>{
    // console.log(answers)
    if (answers.operator === "Addition") {
        console.log(`${answers.num1} + ${answers.num2} = ${answers.num1 + answers.num2}`);
    }
    else if (answers.operator === "Subtraction") {
        console.log(`${answers.num1} - ${answers.num2} = ${answers.num1 - answers.num2}`);
    }
    else if (answers.operator === "Multiplication") {
        console.log(`${answers.num1} * ${answers.num2} = ${answers.num1 * answers.num2}`);
    }
    else if (answers.operator === "Division") {
        console.log(`${answers.num1} / ${answers.num2} = ${answers.num1 / answers.num2}`);
    }
    // })
}
async function thanks() {
    let welocmenote = chalkAnimation.rainbow("thanks for using over service!");
    await sleep();
    welocmenote.stop();
}
async function startAgain() {
    do {
        await askQuestion();
        var again = inquirer.prompt({
            type: "input",
            name: "Restart",
            message: "Do you want to continue press y or n: "
        });
    } while ((await again).Restart === 'y' || (await again).Restart === 'Y' || (await again).Restart === 'Yes' || (await again).Restart === 'yes');
    {
        await thanks();
    }
    ;
}
startAgain();