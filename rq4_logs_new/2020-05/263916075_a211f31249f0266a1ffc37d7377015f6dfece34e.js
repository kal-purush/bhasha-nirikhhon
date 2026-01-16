const painting = document.querySelector('#painting');
const creatediv = document.createElement('div');
let rows = 16;
let divArray = [];

function fillPainting() {

    for(i = 0; i <= rows * rows; i++) {
        
        creatediv.setAttribute('class', 'pixel');
        divArray.push(creatediv);
        
    }
}

fillPainting();