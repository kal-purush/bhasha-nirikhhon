$(document).ready(function() {

//stores input to calculate later

var inputs=[""];
//string to store current input string
var totalString;
  //operators array without .
  var operators1= ["+", "-", "/", "*"];
//operators array with .
  var operators2=["."];

  var nums =[0,1,2,3,4,5,6,7,8,9];

  function getValue(input){
    //dont allow duplicate .
    if(operators2.includes(inputs[inputs.length-1]===true && input===".")) {
      console.log("Duplicat '.' ");
    }
//check if first character is not an operator, if it's not push input
    else if(inputs.length===1 && operators1.includes(input)===false){
      inputs.push(input);
    }
    //dont allow duplicate operators
    else if(operators1.includes(inputs[inputs.length-1])===false){
      inputs.push(input);
    }
    //convert string to number, and push inputs
    else if(nums.includes(Number(input))){
      inputs.push(input);
    }
    update();

  }
  function update(){
    totalString = inputs.join("");
    $("#steps").html(totalString);
  }

  function getTotal(){
    totalString = inputs.join("");
    $("#steps").html(eval(totalString));
  }

//click on anchor tag:
$("a").on("click", function() {
  //if AC is pressed, reset to ""/0
  if(this.id==="deleteAll"){
    inputs=[""];
    update();
  }
  //if CE is pressed, delete last
  else if(this.id==="backOne"){
    inputs.pop();
    update();
  }
  //if = is pressed get total
  else if(this.id==="total"){
    getTotal();
  }
  //
  else{
    //check input of last array, if it is not an operator, get value
    if(inputs[inputs.length-1].indexOf("+", "-", "/", "*", ".")===-1){
      getValue(this.id);
    }
    else{
      getValue(this.id);
    }
  }
});

});