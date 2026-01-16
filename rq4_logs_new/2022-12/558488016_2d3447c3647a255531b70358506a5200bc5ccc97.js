"use strict";


window.onload =  init;



function init()
{
    const estimateButton = document.getElementById("estimateButton");
    estimateButton.onclick = estimateButtonOnclick;
}

function estimateButtonOnclick()
{
    const basicCarRental = 29.99;
    let numberDays = Number(document.getElementById("numberOfDays").value);
    
    let totalCostDays = numberDays * basicCarRental;
    

    let extraPerDay = 0;

if (document.getElementById("tollTag").checked) {
    extraPerDay += 3.95;
}
if (document.getElementById("gps").checked) {
    extraPerDay += 2.95;
}
if (document.getElementById("roadside").checked) {
    extraPerDay += 2.95;
}


let yesRadioBtn = document.getElementById("yes");
let noRadioBtn = document.getElementById("no");
let surcharge = 0;

 if (yesRadioBtn.checked){
    surcharge = basicCarRental * 0.30;
 }
 else if(noRadioBtn.checked){
    surcharge = 0;
 }

    
 
    let total = totalCostDays + (extraPerDay * numberDays) ;
    let output = document.getElementById("output");
    output.innerHTML = "Total Due " + total ;
    
}