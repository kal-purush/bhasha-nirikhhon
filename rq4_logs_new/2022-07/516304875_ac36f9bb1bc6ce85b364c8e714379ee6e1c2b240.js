const target = document.querySelector("#dynamic");
const string = "I want to be a Front-end Developer";
const split = string.split("");


function reset(){
    target.textContent = "";
    const resplit = string.split("");
    dynamic(resplit);
}

function dynamic(arr){
    if(arr.length > 0){
        target.textContent += arr.shift();
        setTimeout(function(){
            dynamic(arr)}, 80)
    } else {
        setTimeout(reset, 3000);
    }
}
dynamic(split);

function blink(){
    target.classList.toggle("active");
}
setInterval(blink, 500)