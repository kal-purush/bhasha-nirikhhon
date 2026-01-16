 window.addEventListener("scroll",function(){
  var navbar=document.getElementById("navbar");
  navbar.classList.toggle("sticky",window.scrollY>0);
  })


document.addEventListener("DOMContentLoaded",init);
function init(){
  let query=window.matchMedia("(max-width:400px)");
  if(query.matches){
    window.addEventListener("scroll",function(){
  var navbar=document.getElementById("navbar");
  navbar.classList.add("fixed",window.scrollY>0);
  })
 }
  else{
   return;
}
}
var w=document.documentElement.clientWidth;
console.log(w);
var button=document.getElementById("menu");
button.addEventListener("click",()=>{
  var newbutton=button.classList.toggle("rotate");
})
button.addEventListener("click",()=>{
  var newmenu=document.getElementById("dropdownmenu");
  newmenu.classList.toggle("show");
});

var anchor1=document.getElementById("anchor1");
anchor1.addEventListener("click",()=>{
  var newmenu=document.getElementById("dropdownmenu");
  newmenu.classList.remove("show");
  var button=document.getElementById("menu");
  button.classList.remove("rotate");
});

var anchor2=document.getElementById("anchor2");
anchor2.addEventListener("click",()=>{
  var newmenu=document.getElementById("dropdownmenu");
  newmenu.classList.remove("show");
  var button=document.getElementById("menu");
  button.classList.remove("rotate");
});

var anchor3=document.getElementById("anchor3");
anchor3.addEventListener("click",()=>{
  var newmenu=document.getElementById("dropdownmenu");
  newmenu.classList.remove("show","rotate");
  var button=document.getElementById("menu");
  button.classList.remove("rotate");
});
