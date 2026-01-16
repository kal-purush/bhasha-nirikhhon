
var togglebtn=document.querySelector(".togglebtn");
var nav=document.querySelector(".navlink");
var link=document.querySelector(".navlink li");

togglebtn.addEventListener("click",function(){
this.classList.toggle("click");
nav.classList.toggle("open");


})
var typed=new Typed(".input",{
    strings:["Frontend Developer"],
    typedSpeed:70,
    backSpeed:55,
    loop:true
})

