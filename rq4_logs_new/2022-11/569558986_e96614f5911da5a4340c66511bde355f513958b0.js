let num1= 8
let num2=2
document.getElementById("num1-el").textContent = num1
document.getElementById("num2-el").textContent = num2
let sum = document.getElementById("sum-el")

function mul(){
sum.textContent="Sum:" + num1*num2
}
function add(){
  let result = num1 + num2  
  sum.textContent="Sum:" + result 
}
function substract(){
    let result = num1 - num2    
sum.textContent="Sum:" + result
}
function divide(){
    sum.textContent="Sum:" + num1/num2
}