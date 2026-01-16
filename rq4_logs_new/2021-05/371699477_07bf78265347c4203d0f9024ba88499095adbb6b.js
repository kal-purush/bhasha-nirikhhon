var addButton = document.getElementById("add-button");
addButton.addEventListener("click", addToDoItem);
function addToDoItem() {
  alert("Add button clicked!");
}

var clearCompleteButton = document.getElementById("clear-completed-button");
clearCompleteButton.addEventListener("click", clearCompletedToDoItem);
function clearCompletedToDoItem() {
  alert("Clear Complted button clicked!");
}

var emptyListButton = document.getElementById("empty-button");
emptyListButton.addEventListener("click", emptyList);
function emptyList() {
  alert("Empty List button clicked!");
}

var saveListButton = document.getElementById("save-button");
saveListButton.addEventListener("click", saveList);
function saveList() {
  alert("Save List button clicked!");
}

var toDoEntryBox = document.getElementById("todo-entry-box");
var toDoList = document.getElementById("todo-list");

function newToDoItem(itemText, completed) {
  var toDoItem = document.createElement("li");
  var toDoText = document.createTextNode(itemText);
  toDoItem.appendChild(toDoText);

  if (completed) {
    toDoItem.classList.add("completed");
  }

  toDoList.appendChild(toDoItem);
  toDoItem.addEventListener("dblclick", toggleToDoItemState);
}

function addToDoItem() {
  var itemText = toDoEntryBox.value;
  newToDoItem(itemText, false);
}

function toggleToDoItemState() {
  if (this.classList.contains("completed")) {
    this.classList.remove("completed");
  } else {
    this.classList.add("completed");
  }
}

function clearCompletedToDoItem() {
  var completedItem = toDoList.getElementsByClassName("completed");

  while (completedItem.length > 0) {
    completedItem.item(0).remove();
  }
}

function emptyList() {
  var toDoItems = toDoList.children;
  while (toDoItems.length > 0) {
    toDoItems.item(0).remove();
  }
}

var myArray = [];
myArray.push("something to store");
myArray.push("something else to store");
alert(myArray[0]);

var toDoInfo = {
  task: "Thing I need to do",
  completed: false
};

function saveList() {
  var toDos = [];

  for (var i = 0; i < toDoList.children.length; i++) {
    var toDo = toDoList.children.item(i);

    var toDoInfo = {
      task: toDo.innerText,
      completed: toDo.classList.contains("completed")
    };

    toDos.push(toDoInfo);
  }

  localStorage.setItem("toDos", JSON.stringify(toDos));
}

function loadList() {
  if (localStorage.getItem("toDos") != null) {
    var toDos = JSON.parse(localStorage.getItem("toDos"));

    for (var i = 0; i < toDos.length; i++) {
      var toDo = toDos[i];
      newToDoItem(toDo.task, toDo.completed);
    }
  }
}

loadList();