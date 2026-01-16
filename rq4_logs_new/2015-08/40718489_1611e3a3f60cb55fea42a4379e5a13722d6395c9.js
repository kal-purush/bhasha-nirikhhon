var downcount = require('./js/downcount.js');
var prompt = require('prompt');

console.log('Welcome to Downcount');

var schema = {
    properties: {
        integers: {
            message: 'Enter a comma seperated list of integers, e.g. 3,5,67,8 and press enter',
            required: true
        },
        targetNumber: {
            message: 'Enter your target number and press enter',
            required: true
        }
    }
};

prompt.start();
var integers;
var targetNumber;
prompt.get(schema, function (err, result) {
    if (err) { return onErr(err); }
    integers = result.integers;
    targetNumber = parseInt(result.targetNumber);
    console.log('The game is to find ' +  targetNumber + ' from the set of numbers ' + integers);

    var solver = new downcount.DownCountSolver();
    var solutions = solver.Solve(targetNumber, integers, false);
    if (solutions.Equations.length > 0) {
        console.log('Downcount found an exact solution');
        console.log(solutions.Equations[0].toString() + ' = ' + solutions.Equations[0].Value());        
    }
    else {
        console.log('Downcount couldn\'t find an excat solution');
    }
});





function onErr(err) {
    console.log(err);
    return 1;
}



