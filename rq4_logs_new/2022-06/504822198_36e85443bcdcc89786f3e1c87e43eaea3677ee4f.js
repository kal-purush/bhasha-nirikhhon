var currentNumberWrapper = document.getElementById('tela')
var currentNumber = 0;

function decremento(){
    currentNumber = currentNumber - 1;
    currentNumberWrapper.innerHTML = currentNumber;
}

function incremento(){
    currentNumber = currentNumber + 1;
    currentNumberWrapper.innerHTML = currentNumber;
}