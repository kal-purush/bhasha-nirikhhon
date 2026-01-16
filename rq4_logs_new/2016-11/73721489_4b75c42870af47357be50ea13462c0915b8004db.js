/*
var myButton = document.createElement("BUTTON");

var myText = document.createTextNode("CLICK ME");

myButton.appendChild(myText);

document.body.appendChild(myButton);

myButton.addEventListener('dblclick', function() {
    document.body.style.backgroundColor = "red";
});*/

var myInput = document.getElementById("input");

var myBtn = document.getElementById("btnAdd");

myBtn.addEventListener("click", function() {
    document.myInput.innerHTML = "I AM THE BEST AROUND!";
});