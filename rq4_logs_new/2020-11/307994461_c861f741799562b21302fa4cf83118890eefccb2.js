const { stringCalc } =  require('./sum')

const inputNumber = process.argv[2];

console.log('Das Ergebnis lautet: ' + stringCalc(inputNumber))