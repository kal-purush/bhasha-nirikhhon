const countValue = document.querySelector('#counter');


//we make simple fuction 
function increment() {
    //get the value from ui
let value = parseInt(countValue.innerText);
//update the value
value = value + 1;
// set the value onto ui
countValue.innerText = value;
};


//we make function using arrow function 
const decrement = () => {
//get the value from ui
let value = parseInt(countValue.innerText);
//update the value
value = value - 1;
// set the value onto ui
countValue.innerText = value;
};