
//  initialize array of n-length
//createArray();     // [] or new Array()
//
//createArray(2);    // new Array(2)
//
//createArray(3, 2); // [new Array(2),
//                   //  new Array(2),
//                   //  new Array(2)]
function createArray(length) {
    var arr = new Array(length || 0),
        i = length;

    if (arguments.length > 1) {
        var args = Array.prototype.slice.call(arguments, 1);
        while(i--) arr[length-1 - i] = createArray.apply(this, args);
    }

    return arr;
}



/////////////////////////////////////////////////////////
// Getting the data from a public accessible-editable spreadsheet
//////////////////////////
 // ID of the Google Spreadsheet
 var spreadsheetIDallRoots = "1UFFKWNgjrsG-7PE-CLdndDw12-hl9WTvo0GChq61Ivw"; 

 var spreadsheetIDsavedRoots = "19YpAGS2kGPfWjZlyXEaYqLY5KavT7YAsPZ-ff_D3X3Y";
 
// initial main dictionary (2d array)
var GLOBAL_dataFromSpreadSheet_allRoots = createArray(4, 4);
var GLOBAL_dataFromSpreadSheet_savedRoots = createArray(2);

 // Make sure it is public or set to Anyone with link can view 
 var url = "https://spreadsheets.google.com/feeds/list/" + spreadsheetIDallRoots + "/od6/public/values?alt=json";
 
 $.getJSON(url, function(data) {
   
   
  var entry = data.feed.entry;
 
   var row=0,col=0;
 

     // _HEADER_
     // _EXAMPLES (should be over 300)
     // ['Root', 'Meaning', 'Origin', 'Example'], 
     // ['a/n', 'not, without', 'Greek', 'abyss..'], 
     // ['acro', 'top, height, tip, beginning', 'Greek', 'acrobat..'], 
		
					// HEADER -- comment if you don't want it
//    var headerRowArray = new Array(4);
//    headerRowArray[0] = 'Root_HEADER';
//    headerRowArray[1] = 'Meaning_HEADER';
//    headerRowArray[2] = 'Origin_HEADER';
//    headerRowArray[3] = 'Example_HEADER';
//    GLOBAL_dataFromSpreadSheet_allRoots[col++] = headerRowArray
    
    
  $(entry).each(function(){
    
    
    
    // pull data from each row entry and insert it into multi-d array
    var currentRowArray = new Array(4);
    currentRowArray[0] = this.gsx$root.$t;
    currentRowArray[1] = this.gsx$meaning.$t;
    currentRowArray[2] = this.gsx$origin.$t;
    currentRowArray[3] = this.gsx$example.$t;
    GLOBAL_dataFromSpreadSheet_allRoots[col++] = currentRowArray;
  });
 
   
     
			console.log("-- Debug: GLOBAL_dataFromSpreadSheet_allRoots");
   console.log(GLOBAL_dataFromSpreadSheet_allRoots);
      
    // TODO - need inputOfWordToFindRoots
//			 var inputOfWordToFindRoots = "myostatin";
//			 var inputOfWordToFindRoots = "allosome ";
//			 var inputOfWordToFindRoots = "angiogenesis";
//			 var inputOfWordToFindRoots = "haplotype";
			 var inputOfWordToFindRoots = "heterosis";
			
			console.log("-- Debug:  findRoots('"+inputOfWordToFindRoots+"')");
			console.log("   Return: findRoots: " + findRoots(inputOfWordToFindRoots));
			
 });
/////////////////////////
//							HELPER FUNCTIONS
/////////////////////////
function findRoots(wordToCheck) {
		var returnValue = "findRootsReturnValue";
	
		// TODO - look up all posibile comibinations of root on this, return all hits
	
	
	// see if root is a subscript of wordToCheck
	for (var i = 0, len = GLOBAL_dataFromSpreadSheet_allRoots.length; i < len; i++) {
		var currentRow = GLOBAL_dataFromSpreadSheet_allRoots[i];
		var currentRowCell0 = currentRow[0];
		var currentRowCell1 = currentRow[1];
		//console.log("-- [Checking] Root: " + currentCell);
		
	
		
		if (currentRowCell0.length > 1 && // skipping single letter
						wordToCheck.indexOf(currentRowCell0) > -1 )
		{
			// TODO - [SPECIAL EXCEPTION] - letter a only, should only be searched against the first letter followed after it has been searched by other a[second letter] choices
			// TODO - [SPECIAL EXCEPTION] - other single letter?
			
			console.log("- [FOUND subString!] " + currentRowCell0);
			console.log("-   Meaning: " + currentRowCell1);
			// TODO_BONUS - add in medical system too (depending if its full match?)
		}
		
	}
	
		//console.log(lookupRoot(wordToCheck));
    
	
  return returnValue;
}