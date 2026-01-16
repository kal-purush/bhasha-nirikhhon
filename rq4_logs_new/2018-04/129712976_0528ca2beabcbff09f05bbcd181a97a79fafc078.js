//------------------Exercise 1-----------------------//
var total= prompt("Enter a number");
for(var num=1; num<=total; num++);{
  total= total + 1
}
console.log("The total is "+ total);
//------------------Exercise 2-----------------------//

var a= prompt("Do you want to play?");
var num=0

do{ 
  var b= prompt("Write a word");
  var c= prompt("Do you want to play again?");
   }while(c!=='no');
   num++;

console.log(b);

//----------------------Exercise 3---------------------------//

var print=prompt("Would you like to print your name?");
while(print==="yes"){
  var name= prompt("What is your name?");
  var print=prompt("Would you like to print again?");

console.log("Hello my name is "+name);
  }



//----------------------Exercise 4----------------------------//

 var a= prompt("What time of day is it?");
 if(a==="morning")
 {
   console.log("Since it is morning, you should be eating breakfast. We suggest eggs and toast.");
 }else if(a==="noon"){
   console.log("Since it is noon you should be eating lunch. We suggest a salad.");
 }else{
   console.log("Since it is evening you should be eating dinner. We suggest chicken and rice.");
 }




	