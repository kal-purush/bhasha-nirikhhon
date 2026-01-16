#! /usr/bin/env node
import chalk from "chalk";
import inquirer from "inquirer";
console.log(chalk.bgGreenBright("**** WelCome Dear Users ****"));
let myBalance = 70000;
let myPin = 54321;
let pinAnswer = await inquirer.prompt([
    {
        name: "pin",
        message: chalk.bgCyanBright("Enter Pin Code!"),
        type: "number",
    }
]);
if (pinAnswer.pin === myPin) {
    console.log(chalk.bgBlackBright("your pin is correct"));
    let operationAns = await inquirer.prompt([
        {
            name: "operation",
            message: chalk.greenBright("Please Select Option"),
            type: "list",
            choices: ["Withdraw", "Fast Cash", "Check Balance"]
        }
    ]);
    if (operationAns.operation === "Withdraw") {
        let amountAns = await inquirer.prompt([
            {
                name: "amount",
                message: chalk.bgGreen("How Much Would you Like To Withdraw"),
                type: "number",
            }
        ]);
        if (amountAns.amount > myBalance) {
            console.log("Your Balance Is Insuficient");
        }
        else if (amountAns.amount == myBalance) {
            console.log("Your Remaining Balance Is: " + 0);
        }
        else if (myBalance -= amountAns.amount) {
            console.log("Your Remaining Balance Is: " + `${myBalance}`);
        }
    }
    else if (operationAns.operation === "Fast Cash") {
        let amountAns = await inquirer.prompt([
            {
                name: "amount",
                message: chalk.bgGreen("How Much Would you Like To Transfer"),
                type: "list",
                choices: [10000, 20000, 30000, 40000]
            }
        ]);
        if (amountAns.amount > myBalance) {
            console.log("Your Balance Is Insuficient");
        }
        else if (amountAns.amount == myBalance) {
            console.log("Your Remaining Balance Is: " + 0);
        }
        else if (myBalance -= amountAns.amount) {
            console.log("Your Remaining Balance Is: " + `${myBalance}`);
        }
        ;
    }
    else if (operationAns.operation === "Check Balance") {
        console.log(`Your Remaining Balance Is ${myBalance}`);
    }
}
else {
    console.log(chalk.redBright("Your PinCode Is Incorrect"));
    console.log(chalk.bgBlack("Please Try Once Again"));
}