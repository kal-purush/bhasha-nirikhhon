// Selecet logo, menu-button, unordered lists, and close-btn

// Select logo
const logo = document.getElementsByClassName("logo");

// Select menu-button
const menuBtn = document.getElementsByClassName("menu-btn");

// select unorderd list
const ulItem = document.getElementById("nav-ul");

// Select close-btn
const closeBtn = document.getElementsByClassName("close-btn");

// Create function that opens unorderd list with logo
// Remember to include index while adding an event listener to class
// Like logo[0]
function displayList(){
    ulItem.classList.toggle("display");
}

logo[0].addEventListener("click",displayList,false);

// Create function that opens unorderd list with menu
// Remember to include index while adding an event listener to class
// Like menuBtn[0]
function displayList(){
    ulItem.classList.toggle("display");
}

menuBtn[0].addEventListener("click",displayList,false);

// Create function that removes unordered list
// Remember to include index while adding an event listener to class
// Like closeBtn[0]
function removeList(){
    ulItem.classList.remove("display");
}

closeBtn[0].addEventListener("click",removeList,false);



