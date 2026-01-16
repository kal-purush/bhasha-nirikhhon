




















// assume the date format is dd/mm/yyyyy
function IsFistLessThanSenondDate(first, second){

	var firstDay = first[0];
	var firstMonth = first[1];
	var firstYear = first[1];

	var secondDay = second[0];
	var secondMonth = second[1];
	var secondYear = second[2];
 
	if(firstYear > secondYear){
		return false;
	}

	// if month greate then no need compare date
	if(firstMonth < secondMonth){
		return true;
	}


	// compare the date which bit different 
	// if the month already great then no need to compare otherwise compare the date
	if(firstDay > secondDay)
	{
		return false;
	}

	return true;
}




















