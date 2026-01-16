
function processData(input) {
    
	var counter = parseInt(input) - 1;

	var craftLine = function(x){
		
		var c = x,
			b = x,
			lineString = "";
		
		while(c > 0){
			lineString += " ";	
			//console.log("#");
			--c;
		}

		while(b <= counter){
			lineString += "#";
			++b;	
		}

		return lineString;
	}

	for(i = counter; i >= 0; --i){
		console.log(craftLine(i));
	} 
	// return f; 
   	// console.log("f: " + f);

}

var output = processData(10);

//console.log(output);
 