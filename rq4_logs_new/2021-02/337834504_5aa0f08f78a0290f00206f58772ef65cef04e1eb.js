const button = document.getElementById("todo_button");
const input = document.getElementById("inputField");
const categoryElt = document.getElementById("category");
const todoList = document.getElementById("todo_list");

// activate the add button if we input a number or a letter
input.addEventListener("input",function(){
	let regex =/\w/;
	button.disabled = !regex.test(input.value);
})

//eventListeners
button.addEventListener("click",addTodo);
todoList.addEventListener("click", deleteCheck);
categoryElt.addEventListener("change", filterTodo)

//if we click tha add button we add an list item with 2 button delete and complete
function addTodo(event){
	event.preventDefault();
		const todoDiv = document.createElement("div");
        todoDiv.classList.add("todo");
		const todoItem = document.createElement("li");
		todoItem.classList.add("todo_item");
		todoItem.textContent = input.value;
		todoDiv.appendChild(todoItem);
		input.value =""	;
		button.disabled = true;

     //add deleteButton and doneButton to todoListItem
		const deleteButton = document.createElement("button");
		deleteButton.id ="deleteButton";
		deleteButton.innerHTML = '<i class="fas fa-trash"></i>';
        deleteButton.classList.add('delete_btn')
		const doneButton = document.createElement("button");
		doneButton.id ="doneButton";
		doneButton.innerHTML = '<i class="fas fa-check"></i>';
        doneButton.classList.add('complete_btn');
        todoDiv.appendChild(doneButton);
		todoDiv.appendChild(deleteButton);
		document.getElementById("todo_list").appendChild(todoDiv);
}
//delete or check an item
function deleteCheck(e){
    const item = e.target;
    const todo = item.parentElement;
    //DELETE ITEM
    if (item.classList[0] === "delete_btn") {
            todo.remove()
      }
    //COMPLETE ITEM
    if (item.classList[0] === "complete_btn") {
        todo.classList.toggle("completedItem")
    }   
}
//categorize the todo items
function filterTodo(e){
	var todos = todoList.childNodes;
	console.log(todos.length);
	for(let i = 0; i<todos.length; i++ ){
        switch (e.target.value) {
            case "All":
                todos[i].style.display = "flex";
                break;
            case "Done":
                if (todos[i].classList.contains('completedItem')) {
                    todos[i].style.display = "flex";
                } else {
                    todos[i].style.display = "none";
                }
                break;
            case "Not Done":
                if (!todos[i].classList.contains('completedItem')) {
                    todos[i].style.display = "flex";
                } else {
                    todos[i].style.display = "none";
                }
                break;
        }
    }
} 