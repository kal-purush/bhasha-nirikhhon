let feild = "";
let buttons = document.querySelectorAll(".but");
    Array.from(buttons).forEach((button)=>{
    button.addEventListener("click", (e)=>{
if(e.target.innerHTML === "="){
    feild = eval(feild);
    document.querySelector("input").value = feild;
}
else if(e.target.innerHTML === "C"){
    feild = "";
    document.querySelector("input").value = feild;
}
else if(e.target.innerHTML === "del"){
    feild =  feild.substring(0, feild.length-1)
    document.querySelector("input").value = feild;
}
else{

feild = feild + e.target.innerHTML;
document.querySelector("input").value = feild;
}

});
  
});