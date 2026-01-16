const demoButton = document.getElementById("demo")//Js object


demoButton.addEventListener("click",()=>{
  document.getElementById("date").innerHTML=`date is:` + Date();
})