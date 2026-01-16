const inputBox = document.getElementById("input-box");
const listContainer = document.getElementById("list-container");
const toggleButton = document.getElementById("theme-toggle"); 
function AddTask(){
    if(inputBox.value === ''){
        alert("Du musst etwas schreiben!!!");
    }
    else{
        let li = document.createElement("li");
        li.innerHTML = inputBox.value;
        listContainer.appendChild(li);
        let span = document.createElement("span");
        span.innerHTML = "\u00d7"; //Cross icon
        li.appendChild(span);
    }
    inputBox.value="";
    SaveData()
}

inputBox.addEventListener("keypress", function(e) {
    if (e.key === "Enter") {
        AddTask();
    }
});

listContainer.addEventListener("click", function(e){
    if(e.target.tagName === "LI"){
        e.target.classList.toggle("checked");
        SaveData()
    }else if (e.target.tagName === "SPAN") { {
        e.target.parentElement.remove();
        SaveData();
    }
}


}, false);
function SaveData(){
    localStorage.setItem("data", listContainer.innerHTML);
}
function DisplayTasks(){
    listContainer.innerHTML= localStorage.getItem("data");
}
DisplayTasks()

function ClearAll() {
    const listContainer = document.getElementById('list-container');
    listContainer.innerHTML = ''; 
    SaveData();
}


