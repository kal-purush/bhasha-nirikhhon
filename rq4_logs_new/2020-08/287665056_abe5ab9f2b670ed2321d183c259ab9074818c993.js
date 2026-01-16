import UIManager from "./UIManager.js";
import { JestEnvironment } from "@jest/environment";
import { jsxEmptyExpression, exportAllDeclaration } from "@babel/types";
import JestMock from "jest-mock";
import path from "path";
import fs from "fs";
const html =
    fs.readFileSync(path.resolve(__dirname, "./index.html"), "utf8");
//import CalculatorParameters from './calculatorParameters.js';
import {
    operationManager
} from './operationManager.js';


// beforeEach(()=>{
//     document.documentElement.innerHTML = html.toString();
//     const calculator = document.querySelector('.calculator');
//     const keys = calculator.querySelectorAll("button");
//     const display = calculator.querySelector(".calculator__display");
//     const displayMini = calculator.querySelector(".calculator__display_mini");
//     const operators = [...calculator.querySelectorAll(".key--operator"), ...calculator.querySelectorAll(".key--equal")];
//     const operatorList = {};
//     operators.forEach(operator => operatorList[operator.dataset.action] = operator.innerHTML);
// })

test('test 1 ', ()=>{
    document.documentElement.innerHTML = html.toString();
    const calculator = document.querySelector('.calculator');
    const keys = calculator.querySelectorAll("button");
    const display = calculator.querySelector(".calculator__display");
    const displayMini = calculator.querySelector(".calculator__display_mini");
    const operators = [...calculator.querySelectorAll(".key--operator"), ...calculator.querySelectorAll(".key--equal")];
    const operatorList = {};
    operators.forEach(operator => operatorList[operator.dataset.action] = operator.innerHTML);
    const parameters = {
        answer :1, 
        strNumber : "23",
        previousOperator: "add", 
        operationBeforeCalculate:"calculate",
        globalConvertNumber:1
    }
    const calculatorUI = new UIManager(calculator, keys, display, displayMini, operatorList, parameters);
    expect(calculatorUI.parameters.answer).toBe(1);
    
 });

