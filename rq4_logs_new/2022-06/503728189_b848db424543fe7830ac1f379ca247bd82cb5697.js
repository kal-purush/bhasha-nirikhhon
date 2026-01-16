
function convertToC(){
   let Farenheit = document.querySelector('#fToC').value;
   let sum1 = (Farenheit - 32) * (5/9);
   document.querySelector('#result1').innerHTML = `Your answer is :
   ${sum1.toFixed(2)} C
    `;
};

function convertToF(){
    let Celsius = document.querySelector('#cToF').value;
    let sum2 = (Celsius * (5*9)) + 32;
    document.querySelector("#result2").innerHTML = `Your answer is :
    ${sum2.toFixed(2)} F
    `;
}


