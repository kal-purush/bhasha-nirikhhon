// CALCULATOR program 

const display = document.getElementById("display");

function apprendToDislay(input){
   display.value += input;
}

function clearDisplay(){
  display.value = "";
}
function calculate() {
  try{
    display.value = eval(display.value);
  }
  catch(error){
    display.value = "Errors"
  }
} 
function del(){
  display.value=display.value.slice(0,-1)
}