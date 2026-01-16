console.log("Hello World!");

// Burger menu

const hamburger = document.getElementById("icon");
const navUL = document.getElementById("nav-links");

hamburger.addEventListener("click", () => {
    navUL.classList.toggle("show");
});

//to do list

const inputBox = document.querySelector(".new-items input");
const addBtn = document.querySelector(".new-items button");
const toDoList = document.querySelector(".items ul");

inputBox.onkeyup = () =>{
    let userData = inputBox.value; //gettin user entered value
    if(userData.trim() !=0){ //if user values are not only spaces
        addBtn.classList.add("active"); //activates the add button
    } 
};
showTasks(); // calling showTasks function

// if user clicks on add button
addBtn.onclick = () =>{
    let userData = inputBox.value; //gettin user entered value
    let getLocalStorage = localStorage.getItem("New Todo"); // getting local storage
    if(getLocalStorage == null){
        listArr = []; // crates blank array
    } else{
        listArr = JSON.parse(getLocalStorage); // transforms json string into a js object
    }
    listArr.push(userData); // pushing or adding user data
    localStorage.setItem("New Todo", JSON.stringify(listArr)); // transfers js object into json string
    showTasks(); // calling showTasks function
};

// function to add task list inside ul
function showTasks(){
    let getLocalStorage = localStorage.getItem("New Todo"); // getting local storage
    if(getLocalStorage == null){
        listArr = []; // crates blank array
    } else{
        listArr = JSON.parse(getLocalStorage); // transforms json string into a js object
    }
    let newLiTag = '';
    listArr.forEach((element, index) => {
        newLiTag += `<li>
        <div class="item">
        <div class="list">
        <p>${element}</p>
        </div>
        <div class="buttons">
        <button onclick="deleteTask(${index})"; class="done"><i class="fas fa-check"></i></button>
        <button onclick="deleteTask(${index})"; class="delete"><i class="far fa-trash-alt"></i></button>
        </div>
        </div>
        </li>`;
    });
    toDoList.innerHTML = newLiTag; // adding new li item
    inputBox.value = ""; // once task added leaves input field blank
}

//delete task function
function deleteTask(index){
    let getLocalStorage = localStorage.getItem("New Todo");
    listArr = JSON.parse(getLocalStorage);
    listArr.splice(index, 1); //delete li
    // after removing li, update local storage
    localStorage.setItem("New Todo", JSON.stringify(listArr)); // transfers js object into json string
    showTasks(); // calling showTasks function
}
