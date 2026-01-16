// Start 30/12/23 13:43

const path = require('path');
const fs = require('fs');

const input = fs
	.readFileSync(path.join(__dirname, 'input.txt'), 'utf8')
	.toString()
	.trim()
    .split('\n')

const matrix = input.map(line => line.split(''))

function extractNumbersFromString(inputString, index) {
    // Use a regular expression to match numbers in the string
    const matches = inputString.match(/\d+/g);
  
    // Convert the matched strings to numbers
    const numbersWithPosition = matches
    ? matches.map(match => ({
        value: Number(match),
        position: inputString.indexOf(match),
        line: index
      }))
    : [];  
    return numbersWithPosition;
  }

function isSymbol(string) {
    return isNaN(string) && string !== '.';
}

function isNumberCloseToASymbol(number, matrix) {
    const {position, line, value} = number;
    const length = String(value).length;
    for (let i = line -1; i < line + 2; i++) {
        for (let j = position -1; j < position + length + 1; j++) {
            if (i < 0 || j < 0 || i > matrix[0].length - 1 || j > matrix.length ) {
                continue;
            }
            // Skipping the number itself
            if(j >= position && j < position + length && i === line) {
                continue;
            }
            if (isSymbol(matrix[i][j])) {
                console.log(number,i, j,matrix[i][j],isSymbol(matrix[i][j]))
                return true
            }
            
        }
    }
    return false
}

const numbers = input
    .map((line,index) => extractNumbersFromString(line,index))
    .flat()

const result = numbers
    .filter(number => isNumberCloseToASymbol(number, matrix))
    .reduce((acc, number) => acc + number.value, 0)

// Part1
const part1 = result
console.log(part1)