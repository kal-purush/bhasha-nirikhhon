function createTable(){
	//This function will make a table with the id multTable, this test to see if there already is a table and clears it if there is
	var table = document.getElementById("multTable");
	if(table != null){
		table.parentNode.removeChild(table);
	}
	//This takes in all the inputs from the form
	var num1 = document.getElementById("num1").value;
	var num2 = document.getElementById("num2").value;
	var num3 = document.getElementById("num3").value;
	var num4 = document.getElementById("num4").value;
	//Pass is used to error check all possibilites for other than numbers
	var pass = 1;
	
	//These if statments check each input as well as the input pairs and correctly changes the heading to state the inputs that didnt not recieve number input
	if(isNaN(num1)){
		document.getElementById("mult1").innerHTML = "Enter Start and End Multiplier: *The first input entered was not a number please try again*";
		pass = 0;
	}
	if(isNaN(num2)){
		document.getElementById("mult1").innerHTML = "Enter Start and End Multiplier: *The second input entered was not a number please try again*";
		pass = 0;
	}
	if(isNaN(num2) && isNaN(num1)){
		document.getElementById("mult1").innerHTML = "Enter Start and End Multiplier: *Both inputs are not numbers please try again*";
		pass = 0;
	}
	if(isNaN(num3)){
		document.getElementById("mult2").innerHTML = "Enter Start and End Multiplicand: *The first input entered was not a number please try again*";
		pass = 0;
	}
	if(isNaN(num4)){
		document.getElementById("mult2").innerHTML = "Enter Start and End Multiplicand: *The second input entered was not a number please try again*";
		pass = 0;
	}
	if(isNaN(num3) && isNaN(num4)){
		document.getElementById("mult2").innerHTML = "Enter Start and End Multiplicand: *Both inputs are not numbers please try again*";
		pass = 0;
	}
	//Pass is default 1, if its not set to 0 by any of the error checking there are no errors then the program continues.
	if(pass){
		//Reset the headings incase someone made a mistake prior we want them to reset to default
		document.getElementById("mult1").innerHTML = "Enter Start and End Multiplier:";
		document.getElementById("mult2").innerHTML = "Enter Start and End Multiplicand:";
		//counter variables
		var i;
		var j;
		//Input order check, this will swap them so the second number is the higher number so it doesnt matter what the user inputs.
		if(+num2 < +num1){
			var temp = num1;
			num1 = num2;
			num2 = temp;
		}
		if(+num4 < +num3){
			var temp = num3;
			num3 = num4;
			num4 = temp;
		}
		//Figure out the width and height of the table
		var width = (+num2 - +num1) + 1;
		var height = (+num4 - +num3) + 1;
		//Create the table at the end of the doucument body with an id multTable
		var tableParent = document.createElement("TABLE");
		tableParent.setAttribute("id", "multTable");
		document.body.appendChild(tableParent);
		//First for loop is the rows, the second is the colums, 3 cases are checked first row first column will be set to nothing, first row will be
		//set to the inputs ie 5,9 will set first row to 5,6,7,8,9 not include row 1 column 1 which is set to nothing as previously mentioned.
		//Then it sets the first column how it set the row just when the column or j is equal to 0, then everything else is the multiplication
		//of each number based on the math (+num1 + j-1)*(+num3 + i-1), this is how you get the values from the first row and first columns
		//while it creates the cell it sets the text then moves on to the next and the program is finished.
		for(i = 0; i <= height; i++){
			var row = tableParent.insertRow(i);
			for(j = 0; j <= width; j++){
				if(i == 0 && j == 0){
					var cell = row.insertCell(j);
					cell.innerHTML = "";
				}else if(i == 0){
					var cell = row.insertCell(j);
					cell.innerHTML = (+num1 + j-1);
				}else if(j == 0){
					var cell = row.insertCell(j);
					cell.innerHTML = (+num3 + i-1);
				}else{
					var cell = row.insertCell(j);
					cell.innerHTML = (+num1 + j-1)*(+num3 + i-1);
				}
				
			}
			
		}

	}
}
	



