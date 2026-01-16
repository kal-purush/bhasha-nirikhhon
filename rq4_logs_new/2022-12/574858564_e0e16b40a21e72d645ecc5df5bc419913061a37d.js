let form_signin = document.getElementById("form1");
let sign_btn = document.getElementById("sign_btn");
let close_btn=document.getElementById("close_btn")
sign_btn.addEventListener("click",()=>{
    form_signin.style.display="flex";
})

close_btn.addEventListener("click",()=>{
    form_signin.style.display="none"
})