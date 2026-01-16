var button = document.getElementById("submitButton");
var input1 = document.getElementById('input1');
var input2 = document.getElementById('input2');
var answer;
var display = document.getElementById('display');
var dropdown = document.getElementById("dropdown");

button.addEventListener("click", doMath);

function doMath() {
    //addition
    if  (dropdown.value == "+"){
        answer = parseInt(input1.value) + parseInt(input2.value);
        display.innerHTML = answer;
    } 
    //subraction
    if(dropdown.value == "-"){
        answer = parseInt(input1.value) - parseInt(input2.value);
        display.innerHTML = answer;
    }
    //divison
    if(dropdown.value == "/"){
        answer = parseInt(input1.value) / parseInt(input2.value);
        display.innerHTML = answer;
    }
    //Muliplication
    if(dropdown.value == "x"){
        answer = parseInt(input1.value) * parseInt(input2.value);
        display.innerHTML = answer;
    }
    //modulus
    if(dropdown.value == "%"){
        answer = parseInt(input1.value) % parseInt(input2.value);
        display.innerHTML = answer;
    }
    //power
    if(dropdown.value == "^"){
        answer = parseInt(input1.value) , parseInt(input2.value);
        display.innerHTML = answer;    
}
}