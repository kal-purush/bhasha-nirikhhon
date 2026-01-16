 //Js for the passenger counting app 
//get the element via DOM. And trun the lement from 0 to 5.
//document.getElementById("count-el").innerText = 5;

//need to create a variable that will hold the count ( number of passengers)

//let count = 5;

//console.log(count);

//Steps to build the passenger counter app

//intiliase the count as 0 to begin with. 
//look for clicks on the button
//increment the button when the button is clicked. 
//change the count-el to reflect the new count.

let countEl = document.getElementById("count-el")
let count = 0;
let saveEl = document.getElementById("save") 
let welcomeEl = document.getElementById("welcome-el")
let myName = "omar"
let greeting = "Welcome back "
welcomeEl.innerText = greeting + " " + myName
welcomeEl.innerText +=  "👋"

function increment(){
    count += 1;
    countEl.innerText = count
}

function save() {
    let counterSave = count + "-"
    saveEl.textContent += counterSave
    countEl.textContent = 0
    count = 0
}

function reset() {
    saveEl.textContent = "Previous entries: ";
    countEl.textContent = 0
    count = 0
}