function countAllFromTown(str1,str2){
	var myString = str1.split(",");
  var myString2 = [];
  //Loop into the string myString
  for(var i = 0; i < myString.length; i++){
  	if(myString[i].trim().startsWith(str2)){
    	myString2.push(myString[i].trim());
    }
   
  }
  return myString2.length;
}