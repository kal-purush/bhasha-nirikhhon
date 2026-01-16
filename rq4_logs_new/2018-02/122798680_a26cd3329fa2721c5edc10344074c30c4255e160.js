function underToCamel(str){
  
  str = str.toLowerCase();
  
  var underscore = "_";
  
  for(x = 0; x < str.length; x++){
   
    while(underscore.indexOf(str[x]) > -1){
      
      str = str.slice(0, x) + str[x+1].toUpperCase() + str.slice((x+2));
   
    }

  }
  
  return str;
  
}

console.log(underToCamel("Camel_Case_to___Under_scorE"));