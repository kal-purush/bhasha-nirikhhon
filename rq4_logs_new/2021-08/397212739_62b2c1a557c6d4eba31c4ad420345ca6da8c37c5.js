const inputValue = document.getElementById('result');

function insertNumber(number){ 
    inputValue.value = inputValue.value + number ;
} 

function clearReault(){
    inputValue.value = '';
}

function calculate(){
    const result = eval(inputValue.value);
    inputValue.value = result;
}

function deleteNumber(){
    const numbers = inputValue.value;
    if(numbers != ''){
        inputValue.value = numbers.slice(0, -1);
    }
}

function squreRoot(){
    const rootOver = Math.sqrt(inputValue.value);
    inputValue.value = rootOver;
}