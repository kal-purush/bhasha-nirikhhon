const form = document.getElementById("form");
const input = document.getElementById("input");
const ul = document.getElementById("ul");

form.addEventListener("submit", function (event){
    event.preventDefault();
    let temp = input.value;
    
    if(!temp) return;
    const li = document.createElement("li");
    
    li.innerText = temp;
    li.classList.add("list-group-item")
    
    li.addEventListener("contextmenu", function (event) {
        event.preventDefault();
        li.remove();
    });
    
    li.addEventListener("click", function () {
       li.classList.toggle("text-decoration-line-through");
    });
    
    ul.appendChild(li);
    input.value = "";
});