//function to add tasks
function addTask() {
    const taskInput = document.getElementById("taskInput");
    const taskList = document.getElementById("taskList");
    const inputError = document.getElementById("input-container").nextElementSibling;
    

    const taskValue = taskInput.value.trim();
    if (taskValue === "") {
        inputError.innerHTML= "Please enter a task!";
        inputError.style.color = "red";
        inputError.style.display = "block";
        return;
    }

//create a new list item
const li = document.createElement("li");
li.textContent = `${taskValue}`;
const listItem = document.getElementById("list-item");

listItem.innerHTML= `${taskValue}`;
// taskList.append(li);

//add a delete button to the task
const deleteButton = document.createElement("button");
deleteButton.textContent = "Delete";
deleteButton.className = "delete-btn";
deleteButton.onclick = function () {
    taskList.removeChild(li);
}

li.appendChild(deleteButton);
// taskList.appendChild(li);

//clear the input field
taskInput.value = "";
}
