let myImage = document.querySelector('img');

myImage.onclick = function() {
    let mySrc = myImage.getAttribute('src');
    if(mySrc === 'WEBimages/ram.png'){
        myImage.setAttribute('src', 'WEBimages/ram2.jpg');
    } else {
        myImage.setAttribute('src', 'WEBimages/ram.png');
    }
}
let myButton = document.querySelector('button');
let myHeading = document.querySelector('h1');

function setUserName() {
    let myName = promt('Please enter your name.');
    if(!myName) {
        setUserName();
    } else {
    localStorage.setItem('name', myName);
    myHeading.textContent = 'Rams are cool, ' + myName;
    }
}

if(!localStorage.getItem('name')) {
    setUserName();
} else {
    let storedName = localStorage.getItem('name');
    myHeading.textContent = 'Rams are cool, ' + storedName;
}

myButton.onclick = function() {
    setUserName();
}