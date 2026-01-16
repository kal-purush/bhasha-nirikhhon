const form = document.querySelector("#form");
const input = document.querySelector("#input");
const errorText = document.querySelector("#error-text")

form.addEventListener("submit",(e)=>{
  e.preventDefault();
  // alert(input.value);
  if(input.value.includes(".com") && input.value.includes("@")){

  input.classList.remove("error");
  errorText.classList.add("hide-error");

  }
  else{
    input.classList.add("error");
    errorText.classList.remove("hide-error");
  }
  input.value="";
})