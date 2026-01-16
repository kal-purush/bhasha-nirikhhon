var PEG = require('pegjs')
   ,fs = require('fs');

var ROWS = 6;
var COLS = 50;


var rawInput = fs.readFileSync(__dirname+'/input', 'utf-8').trim();


var grammar = fs.readFileSync(__dirname+'/grammar', 'utf-8');
var parser = PEG.generate(grammar);
var input = parser.parse(rawInput);
// console.log(input);


function initLcd(rows, cols, item){
	var arr = [];
	for(var row=0; row<ROWS; row++){
		for(var col=0; col<COLS; col++){
			if(!arr[row]) arr[row] = [];
			arr[row][col] = 0;
		}
	}
	return arr;
}

function showLcd(lcd, msg){
	console.log(msg);
	for(var row=0; row<ROWS; row++){
		var str = '';
		for(var col=0; col<COLS; col++){
			str += lcd[row][col] ? '#' : '.';
		}
		console.log(str);
	}
	// console.log(lcd);
}

function rect(lcd, x, y){
	for(var row=0; row<y; row++){
		for(var col=0; col<x; col++){
			lcd[row][col] = 1;
		}
	}
	return lcd;
}

function rotateRow(lcd, y, num){
	var newRow = [];
	for (var col=0; col<COLS; col++){
		var newCol = col+num;
		while(newCol>=COLS){
			newCol -= COLS;
		}
		newRow[newCol] = lcd[y][col];
	}
	lcd[y] = newRow;
	return lcd;
}

function rotateCol(lcd, x, num){
	var newLcd = JSON.parse(JSON.stringify(lcd)); // clone
	for (var row=0; row<ROWS; row++){
		var newRow = row+num;
		while(newRow>=ROWS){
			newRow -= ROWS;
		}
		newLcd[newRow][x] = lcd[row][x];
	}
	return newLcd;	
}

function countPixels(lcd){
	var sum = 0;
	for(var row=0; row<ROWS; row++){
		for(var col=0; col<COLS; col++){
			if (lcd[row][col]) sum++;
		}
	}
	return sum;
}


// ---------
// test data
// ---------

// ROWS = 3;
// COLS = 7;
// var lcd = initLcd();
// showLcd(lcd, 'initial state:');

// lcd = rect(lcd, 3, 2);
// showLcd(lcd, 'rect 3 2');

// lcd = rotateCol(lcd, 1, 1);
// showLcd(lcd, 'rotate col 1 1');

// lcd = rotateRow(lcd, 0, 4);
// showLcd(lcd, 'rotate row 0 4');

// lcd = rotateCol(lcd, 1, 1);
// showLcd(lcd, 'rotate col 1 1');



// ---------
// real data
// ---------
var lcd = initLcd();
showLcd(lcd, 'initial state:');

for(var i in input){
	var rule = input[i];
	//console.log('rule: ', rule);
	if(rule.type == 'Rect'){
		lcd = rect(lcd, rule.x, rule.y);
	}
	if(rule.type == 'RotateRow'){
		lcd = rotateRow(lcd, rule.y, rule.num);
	}
	if(rule.type == 'RotateCol'){
		lcd = rotateCol(lcd, rule.x, rule.num);
	}
}

showLcd(lcd, 'end state:');
console.log('countPixels', countPixels(lcd));