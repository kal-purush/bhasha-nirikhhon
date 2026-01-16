/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
var app = {
    // Application Constructor
    initialize: function() {
        document.addEventListener('deviceready', this.onDeviceReady.bind(this), false);
    },

    // deviceready Event Handler
    //
    // Bind any cordova events here. Common events are:
    // 'pause', 'resume', etc.
    onDeviceReady: function() {
        this.receivedEvent('deviceready');
    },

    // Update DOM on a Received Event
    receivedEvent: function(id) {
        var parentElement = document.getElementById(id);
        var listeningElement = parentElement.querySelector('.listening');
        var receivedElement = parentElement.querySelector('.received');

        listeningElement.setAttribute('style', 'display:none;');
        receivedElement.setAttribute('style', 'display:block;');

        console.log('Received Event: ' + id);
    }
	
};

//Initialisation of variables
var d4Quan = 0;
var d6Quan = 0;
var d8Quan = 0;
var d10Quan = 0;
var dPercentileQuan = 0;
var d12Quan = 0;
var d20Quan = 0;
var outputTable = [''];
var dieArray = ['label'];
var result = [0];
var total = 0;
var totalQuantity = 0;
var tableRow = 1;

//The main random number generation function
function NumRand(low, range) {
	var answer = Math.floor(Math.random()*range) + low;
	return answer;
}

//Checks the are 0s high checkbox
function IsZeroHigh() {
	var checkBox = document.getElementById("zeroHighCheck");
	if (checkBox.checked == true){
		return true;
	} else {
		return false;
	}
}

//Checks whether the Stunt Die checkbox is checked
function IsStuntDie() {
	var checkBox = document.getElementById("stuntDieCheck");
	if (checkBox.checked == true){
		return true;
	} else {
		return false;
	}
}

//resets the output display and the various arrays are returned to their initial value
function OutputDisplayReset() {
	$("#output").text('');
	total = 0;
	tableRow = 1;
	outputTable.length = 1;
	dieArray.length = 1;
	result.length = 1;
}

//Prints the output table
function OutputDisplayUpdate() {
	var outputText = '';
	
	//Calculates the total
	for (var i = 0; i < result.length; i++){
		total += result[i];
	}
	
	//If more than one dice involved, prints the total as the final row of the table
	if (totalQuantity > 1) {
		outputTable.push('<tr><td>Total =</td><td>' + total + '</td><td></td></tr></tbody></table>');
	} else {
		outputTable.push('</tbody></table>');
	}
	
	//COnverts the output table into a single string
	for (var i = 0; i < outputTable.length; i++){
		outputText += outputTable[i];
	}
	
	//Places that string into the output <div> as html
	$("#output").html(outputText);
}

//Handles the '+' button in the HTML, taking the argument from the onclick
function addDie(value){
	switch(value) {
		case 4:
		d4Quan += 1;
		break;
		case 6:
		d6Quan += 1;
		break;
		case 8:
		d8Quan += 1;
		break;
		case 10:
		d10Quan += 1;
		break;
		case 100:
		dPercentileQuan += 1;
		break;
		case 12:
		d12Quan += 1;
		break;
		case 20:
		d20Quan += 1;
		break;
		default:
	}
	UpdateQuantity();
	return false;
}

//Handles the '-' button in the HTML, taking the argument from the onclick
function removeDie(value){
	switch(value) {
		case 4:
		if(d4Quan > 0) d4Quan -= 1;
		break;
		case 6:
		if(d6Quan > 0) d6Quan -= 1;
		break;
		case 8:
		if(d8Quan > 0) d8Quan -= 1;
		break;
		case 10:
		if(d10Quan > 0) d10Quan -= 1;
		break;
		case 100:
		if(dPercentileQuan > 0) dPercentileQuan -= 1;
		break;
		case 12:
		if(d12Quan > 0) d12Quan -= 1;
		break;
		case 20:
		if(d20Quan > 0) d20Quan -= 1;
		break;
		default:
	}
	UpdateQuantity();
	return false;
}

//Rolls a single die. The -1 Quntity means that the code knows it's a single dice roll to be performed instantly
function roll(value){
	switch(value) {
		case 4:
		rolld4(-1);
		break;
		case 6:
		rolld6(-1);
		break;
		case 8:
		rolld8(-1);
		break;
		case 10:
		rolld10(-1);
		break;
		case 100:
		rolldPercentile(-1);
		break;
		case 12:
		rolld12(-1);
		break;
		case 20:
		rolld20(-1);
		break;
		default:
	}
}

//Rerolls a single die. First finds the label of that row, and then asks for a reroll of that die type. Quantity -2 passes information that it's a reroll to the rest of the code
function Reroll(rowNumber) {
	switch (dieArray[rowNumber]) {
		case 'd4':
		rolld4(-2,rowNumber);
		break;
		case 'd6':
		rolld6(-2,rowNumber);
		break;
		case 'd8':
		rolld8(-2,rowNumber);
		break;
		case 'd10':
		rolld10(-2,rowNumber);
		break;
		case 'd%':
		rolldPercentile(-2,rowNumber);
		break;
		case 'd12':
		rolld12(-2,rowNumber);
		break;
		case 'd20':
		rolld20(-2,rowNumber);
		break;
		case 'Stunt':
		rolld6(-3,rowNumber);
		break;
		default:
	}
}

//Rolls all of the selected dice.
function GenerateSelected() {
	//reset the display
	OutputDisplayReset();
	
	//set first row of output table
	outputTable[0] = '<table><tbody><tr><th>Die</th><th colspan = 2>Result</th></tr>';
	
	//rolls each die the correct number of times, with no preset row
	rolld20(d20Quan,0);
	rolld12(d12Quan,0);
	rolldPercentile(dPercentileQuan,0);
	rolld10(d10Quan,0);
	rolld8(d8Quan,0);
	rolld6(d6Quan,0);
	rolld4(d4Quan,0);
	
	//Update the output display
	OutputDisplayUpdate();
}

//Updates all of the totals for the quantities in the HTML display so the user can see the quantities selected
function UpdateQuantity () {
	
	$("#d4Quan").text(d4Quan);
	$("#d6Quan").text(d6Quan);
	$("#d8Quan").text(d8Quan);
	$("#d10Quan").text(d10Quan);
	$("#dPercentileQuan").text(dPercentileQuan);
	$("#d12Quan").text(d12Quan);
	$("#d20Quan").text(d20Quan);

}

//Rolls the specified die the specified number of times
function GenerateDie(low, high, label, quan) {
	var range = high - low + 1;
	
	//Check if the Stunt Die is being used
	if (IsStuntDie() && quan == 3 && high == 6 && totalQuantity == 3) {
		
		//Label first Die the Stunt Die
		var numGenerated = NumRand(low, range).toString();
		outputTable.push('<tr><td>Stunt</td><td>' + numGenerated + '</td><td><button onclick="Reroll(' + tableRow +')">Reroll</button></td></tr>');
		result.push(Number(numGenerated));
		dieArray.push('Stunt');
		tableRow += 1;
		
		//Label the rest as normal	
		for (var i = 1; i < quan; i++){
			var numGenerated = NumRand(low, range).toString();
			outputTable.push('<tr><td>' + label + '</td><td>' + numGenerated + '</td><td><button onclick="Reroll(' + tableRow +')">Reroll</button></td></tr>');
			result.push(Number(numGenerated));
			dieArray.push(label);
			tableRow += 1;
		}
	} else if (quan == 0){
		return false;
	} else {
		//Roll the required number of dice
		for (var i = 0; i < quan; i++){
			var numGenerated = NumRand(low, range).toString();
			outputTable.push('<tr><td>' + label + '</td><td>' + numGenerated + '</td><td><button onclick="Reroll(' + tableRow +')">Reroll</button></td></tr>');
			result.push(Number(numGenerated));
			dieArray.push(label);
			tableRow += 1;
		}
	}
	return false;
}

//Rerolls the specified die
function GenerateReroll(low, high, label, row) {
	var range = high - low + 1;
	
	//empty the total and remove the final row of the output table (does not reset arrays)
	total = 0;
	outputTable.pop();
	
	//Generate a random number
	var numGenerated = NumRand(low, range).toString();
	
	//Rewrite the appropriate row and replace that value in the results array
	outputTable[row] = '<tr><td>' + label + '</td><td>' + numGenerated + '</td><td><button onclick="Reroll(' + row +')">Reroll</button></td></tr>';
	result[row] = Number(numGenerated);
	
	//Update the display
	OutputDisplayUpdate()
	
}

//Generates the code for a single die roll
function GenerateSingleDice(low, high, label) {
	OutputDisplayReset();
	totalQuantity = 0;
	
	//create opening html tags and header
	outputTable[0] = '<table><tbody><tr><th>Die</th><th colspan = 2>Result</th></tr>';
	
	//generate a die and add it's row to the html
	GenerateDie(low, high, label, 1);
	
	//close the html
	outputTable.push('</tbody></table>');
	
	//Update the display
	OutputDisplayUpdate();

}

//All of these functions call the appropriate function, having set the parameters for the die sizeToContent
//Quantity -1 means roll a single die ignoring the currently selected quanities
//Quantity -2 means reroll
//Row variable is only used in rerolls
function rolld4(quan, row){
	var low = 1;
	var high = 4;
	var label = 'd4';
	if(quan == -1){
		GenerateSingleDice(low, high, label);
	} else if (quan == -2) {
		GenerateReroll(low, high, label, row)
	} else {
		GenerateDie(low, high, label, quan);
	}
	return false;
}

function rolld6(quan, row){
	var low = 1;
	var high = 6;
	var label = 'd6';
	if(quan == -1){
		GenerateSingleDice(low, high, label);
	} else if (quan == -2) {
		GenerateReroll(low, high, label, row)
	} else if (quan == -3) {
		label = 'Stunt';
		GenerateReroll(low, high, label, row)
	} else {
		GenerateDie(low, high, label, quan);
	}
	return false;
}

function rolld8(quan, row){
	var low = 1;
	var high = 8;
	var label = 'd8';
	if(quan == -1){
		GenerateSingleDice(low, high, label);
	} else if (quan == -2) {
		GenerateReroll(low, high, label, row)
	} else {
		GenerateDie(low, high, label, quan);
	}
	return false;
}

function rolld10(quan, row){
	var label = 'd10';
	if (IsZeroHigh()) {
		var low = 1;
		var high = 10;
	} else {
		var low = 0;
		var high = 9;
	}
	if(quan == -1){
		GenerateSingleDice(low, high, label);
	} else if (quan == -2) {
		GenerateReroll(low, high, label, row)
	} else {
		GenerateDie(low, high, label, quan);
	}
	return false;
}

function rolldPercentile(quan, row){
	var label = 'd%';
	if (IsZeroHigh()) {
		var low = 1;
		var high = 100;
	} else {
		var low = 0;
		var high = 99;
	}
	if(quan == -1){
		GenerateSingleDice(low, high, label);
	} else if (quan == -2) {
		GenerateReroll(low, high, label, row)
	} else {
		GenerateDie(low, high, label, quan);
	}
	return false;
}

function rolld12(quan, row){
	var low = 1;
	var high = 12;
	var label = 'd12';
	if(quan == -1){
		GenerateSingleDice(low, high, label);
	} else if (quan == -2) {
		GenerateReroll(low, high, label, row)
	} else {
		GenerateDie(low, high, label, quan);
	}
	return false;
}

function rolld20(quan, row){
	var low = 1;
	var high = 20;
	var label = 'd20';
	if(quan == -1){
		GenerateSingleDice(low, high, label);
	} else if (quan == -2) {
		GenerateReroll(low, high, label, row)
	} else {
		GenerateDie(low, high, label, quan);
	}
	return false;
}


$(function(){

	//Dice roller
	$("#rollSelected").click(function() {
		totalQuantity = d4Quan + d6Quan + d8Quan + d10Quan + dPercentileQuan + d12Quan + d20Quan;
		GenerateSelected();
		return false;
		
	});
	
	//Clear function
	$("#clear").click(function() {
		d4Quan = d6Quan = d8Quan = d10Quan = dPercentileQuan = d12Quan = d20Quan = 0;
		UpdateQuantity();
		OutputDisplayReset();
		return false;
		
	});
		
});


app.initialize();