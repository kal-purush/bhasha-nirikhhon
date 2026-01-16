const btnsubir = document.getElementById("btn1"); 
const  btndescer = document.getElementById("btn3");
const btnzerar = document.getElementById("btn2");
const span = document.getElementById ("zero");


let zero = 0;


btnsubir.addEventListener("click", function(){
    span.innerHTML = ++zero;
});

btndescer.addEventListener("click" , function() {
    span.innerHTML = --zero;
});

    btnzerar.addEventListener("click" , function(){
        span.innerHTML = zero = 0;
    });
