//This uses recursion to solve traditional problems involving string reversal

//Normal approach normallu




function reverseRecursion(string){
  if(string === ""){
    return "";
  }else{
    return reverseRecursion(string.substr(1)) + string.charAt(0); 
  }
}


console.log(reverseRecursion("hello") === "olleh");
//this will print true because we are successful