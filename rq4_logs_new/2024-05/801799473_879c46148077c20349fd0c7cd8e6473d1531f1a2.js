const addTask = () => {
  let taskInput = document.getElementById("taskInput");
  let taskList = document.getElementById("taskList");

  if (taskInput.value === "") {
    alert("please enter task");
  } 
  else {
    let li = document.createElement("li");
    li.textContent = taskInput.value;

    let deleteButton = document.createElement("span");
    deleteButton.textContent = "X";
    deleteButton.className = "delete";
    deleteButton.onclick = function () {
      li.remove();
    };

    li.appendChild(deleteButton);
    taskList.appendChild(li);

    taskInput.value = "";
  }
};
console.log(2 + 3);